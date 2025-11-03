"""
Atelier Absences — V1.1 (Windows 7 compatible)
Author: ChatGPT (GPT‑5 Thinking)

Mises à jour (v1.1)
- **Présence** :
  • Correction clic par employé : un clic ne modifie **que** la ligne concernée.
  • Les options **PM** (Après‑midi) **n'apparaissent que** si l'on a cliqué sur **Activer Demi‑journée** (et non plus automatiquement après 10:00).
  • Suppression du **clignotement** : la liste n'est plus rerendue chaque seconde ; seul l'état du bouton Demi‑journée est rafraîchi.
  • Mise en page **tableau aligné** (grille à 4 colonnes) : Employé | Matin | Après‑midi | Statut.
- **Menu principal** : boutons **beaucoup plus grands**, texte **gras**, expansion plein espace.
- **Paramètres / Jours & Heures** : correction d'alignement ; **Lundi** réaligné (marge homogène).

Lancer
    python atelier_absences_v1.py

Dépendances
- Standard: Tkinter, sqlite3
- Optionnel: openpyxl (export Excel) ; certifi (TLS Win7 pour fériés)
"""

import os
import sys
import sqlite3
import csv
import json
import ssl
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from datetime import datetime, date, time, timedelta
from typing import Optional, List, Tuple

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog, simpledialog
except Exception as e:
    raise SystemExit("Tkinter est requis.")

DB_PATH = os.path.join(os.path.dirname(__file__), "atelier_absences.db")
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"

# -------------------- Utilitaires --------------------

def today_local() -> date:
    return date.today()

def now_local() -> datetime:
    return datetime.now()

def parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s.strip(), DATE_FMT).date()
    except Exception:
        return None

# -------------------- DB --------------------
SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    am_status TEXT DEFAULT 'none',
    am_time TEXT,
    pm_status TEXT DEFAULT 'none',
    pm_time TEXT,
    UNIQUE(employee_id, date),
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS holidays (
    date TEXT PRIMARY KEY,
    label TEXT,
    is_working_day INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS day_flags (
    date TEXT PRIMARY KEY,
    halfday_applied INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS advances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    taken_at TEXT NOT NULL,
    comment TEXT,
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS salary_profiles (
    employee_id INTEGER PRIMARY KEY,
    base_salary REAL DEFAULT 0,
    bonus REAL DEFAULT 0,
    malus REAL DEFAULT 0,
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
);
"""

DEFAULT_SETTINGS = {
    "workdays": "Mon,Tue,Wed,Thu,Fri",
    "arrival_time": "08:00",
    "halfday_pivot": "10:00",
    "admin_password": "",
    "late_penalty_amount": "0"
}

class DB:
    def __init__(self, path: str = DB_PATH):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self.conn.executescript(SCHEMA)
        for k, v in DEFAULT_SETTINGS.items():
            self.set_if_empty(k, v)
        self.conn.commit()

    def set_if_empty(self, key: str, value: str):
        cur = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        if not cur.fetchone():
            self.conn.execute("INSERT INTO settings(key,value) VALUES(?,?)", (key, value))

    # settings
    def get(self, key: str, default: str = "") -> str:
        r = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else default

    def set(self, key: str, value: str):
        self.conn.execute("REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
        self.conn.commit()

    # employees
    def employees(self) -> List[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM employees ORDER BY name"))

    def add_employee(self, name: str):
        self.conn.execute("INSERT INTO employees(name) VALUES(?)", (name,))
        self.conn.commit()

    def rename_employee(self, emp_id: int, new_name: str):
        self.conn.execute("UPDATE employees SET name=? WHERE id=?", (new_name, emp_id))
        self.conn.commit()

    def delete_employee(self, emp_id: int):
        self.conn.execute("DELETE FROM employees WHERE id=?", (emp_id,))
        self.conn.commit()

    # holidays
    def list_holidays(self) -> List[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM holidays ORDER BY date"))

    def add_holiday(self, d: date, label: str, is_working_day: bool):
        self.conn.execute(
            "REPLACE INTO holidays(date,label,is_working_day) VALUES(?,?,?)",
            (d.strftime(DATE_FMT), label, 1 if is_working_day else 0),
        )
        self.conn.commit()

    def remove_holiday(self, d: date):
        self.conn.execute("DELETE FROM holidays WHERE date=?", (d.strftime(DATE_FMT),))
        self.conn.commit()

    # day flags
    def get_halfday_flag(self, d: date) -> int:
        r = self.conn.execute("SELECT halfday_applied FROM day_flags WHERE date=?", (d.strftime(DATE_FMT),)).fetchone()
        return int(r[0]) if r else 0

    def set_halfday_flag(self, d: date, val: int):
        self.conn.execute("REPLACE INTO day_flags(date,halfday_applied) VALUES(?,?)", (d.strftime(DATE_FMT), int(val)))
        self.conn.commit()

    # advances
    def add_advance(self, employee_id: int, amount: float, taken_at: Optional[datetime] = None, comment: Optional[str] = None):
        ts = (taken_at or datetime.now()).replace(microsecond=0).isoformat()
        self.conn.execute(
            "INSERT INTO advances(employee_id,amount,taken_at,comment) VALUES(?,?,?,?)",
            (employee_id, float(amount), ts, comment),
        )
        self.conn.commit()

    def delete_advance(self, advance_id: int):
        self.conn.execute("DELETE FROM advances WHERE id=?", (advance_id,))
        self.conn.commit()

    def advances_for_employee(self, employee_id: int) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM advances WHERE employee_id=? ORDER BY taken_at DESC, id DESC",
                (employee_id,),
            )
        )

    def advances_totals(self) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT e.id AS employee_id, e.name, COALESCE(SUM(a.amount), 0) AS total
                FROM employees e
                LEFT JOIN advances a ON a.employee_id = e.id
                GROUP BY e.id
                ORDER BY e.name
                """
            )
        )

    def _month_bounds(self, year: int, month: int) -> Tuple[str, str]:
        start = date(year, month, 1)
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        return (
            f"{start.strftime(DATE_FMT)}T00:00:00",
            f"{next_month.strftime(DATE_FMT)}T00:00:00",
        )

    def monthly_advances(self, year: int, month: int) -> List[sqlite3.Row]:
        start, end = self._month_bounds(year, month)
        return list(
            self.conn.execute(
                """
                SELECT e.id AS employee_id, e.name, COALESCE(SUM(a.amount), 0) AS total
                FROM advances a
                JOIN employees e ON e.id = a.employee_id
                WHERE a.taken_at >= ? AND a.taken_at < ?
                GROUP BY e.id
                ORDER BY e.name
                """,
                (start, end),
            )
        )

    # salary profiles
    def ensure_salary_profile(self, employee_id: int):
        self.conn.execute(
            "INSERT OR IGNORE INTO salary_profiles(employee_id, base_salary, bonus, malus) VALUES(?,?,?,?)",
            (employee_id, 0.0, 0.0, 0.0),
        )

    def get_salary_profile(self, employee_id: int) -> sqlite3.Row:
        self.ensure_salary_profile(employee_id)
        return self.conn.execute(
            "SELECT base_salary, bonus, malus FROM salary_profiles WHERE employee_id=?",
            (employee_id,),
        ).fetchone()

    def update_salary_profile(self, employee_id: int, **fields):
        allowed = {"base_salary", "bonus", "malus"}
        updates = []
        params = []
        for key, value in fields.items():
            if key in allowed:
                updates.append(f"{key}=?")
                params.append(float(value))
        if not updates:
            return
        self.ensure_salary_profile(employee_id)
        params.append(employee_id)
        self.conn.execute(
            f"UPDATE salary_profiles SET {', '.join(updates)} WHERE employee_id=?",
            params,
        )
        self.conn.commit()

    def salary_profiles_map(self) -> dict:
        cur = self.conn.execute("SELECT employee_id, base_salary, bonus, malus FROM salary_profiles")
        return {row["employee_id"]: row for row in cur}

    # attendance
    def ensure_day(self, d: date):
        emps = self.employees()
        for r in emps:
            self.conn.execute("INSERT OR IGNORE INTO attendance(employee_id,date) VALUES(?,?)", (r["id"], d.strftime(DATE_FMT)))
        self.conn.commit()

    def day_rows(self, d: date) -> List[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT a.*, e.name FROM attendance a JOIN employees e ON e.id=a.employee_id WHERE date=? ORDER BY e.name",
            (d.strftime(DATE_FMT),)
        ))

    def set_am(self, employee_id: int, d: date, status: str, t: Optional[datetime]):
        tstr = t.strftime(TIME_FMT) if t else None
        self.conn.execute("UPDATE attendance SET am_status=?, am_time=? WHERE employee_id=? AND date=?",
                          (status, tstr, employee_id, d.strftime(DATE_FMT)))
        self.conn.commit()

    def set_pm(self, employee_id: int, d: date, status: str, t: Optional[datetime]):
        tstr = t.strftime(TIME_FMT) if t else None
        self.conn.execute("UPDATE attendance SET pm_status=?, pm_time=? WHERE employee_id=? AND date=?",
                          (status, tstr, employee_id, d.strftime(DATE_FMT)))
        self.conn.commit()

# -------------------- App --------------------
class App(tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        self.title("Atelier Absences — V1.1")
        self.geometry("1100x680")
        self.resizable(True, True)
        self.db = DB(DB_PATH)

        # Styles (boutons plus grands et gras)
        style = ttk.Style(self)
        style.configure("Big.TButton", font=("Segoe UI", 18, "bold"), padding=(26, 30))

        wrap = ttk.Frame(self)
        wrap.pack(expand=True, fill=tk.BOTH)
        ttk.Label(wrap, text="Menu Principal", font=("Segoe UI", 20, "bold")).pack(pady=18)

        grid = ttk.Frame(wrap)
        grid.pack(pady=24, expand=True)
        for i in range(4):
            grid.grid_columnconfigure(i, weight=1, uniform="btns", minsize=220)
        grid.grid_rowconfigure(0, weight=1)

        def bigbtn(text, cmd, col):
            b = ttk.Button(grid, text=text, command=cmd, style="Big.TButton", width=16)
            b.grid(row=0, column=col, padx=30, pady=18)

        bigbtn("Employés", self.open_employees, 0)
        bigbtn("Présence", self.open_presence, 1)
        bigbtn("Paramètres", self.open_settings, 2)
        bigbtn("Salaires", self.open_salaries, 3)

    def open_employees(self):
        EmployeesWin(self, self.db)

    def open_presence(self):
        PresenceWin(self, self.db)

    def open_settings(self):
        SettingsWin(self, self.db)

    def open_salaries(self):
        SalariesWin(self, self.db)

# ---------- Employés ----------
class EmployeesWin(tk.Toplevel):
    def __init__(self, master, db: DB):
        tk.Toplevel.__init__(self, master)
        self.db = db
        self.title("Employés")
        self.geometry("640x460")
        self.build()
        self.refresh()

    def build(self):
        top = ttk.Frame(self)
        top.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.tree = ttk.Treeview(top, columns=("name",), show="headings", height=14)
        self.tree.heading("name", text="Nom")
        self.tree.pack(fill=tk.BOTH, expand=True)
        btns = ttk.Frame(self)
        btns.pack(fill=tk.X, padx=10, pady=8)
        ttk.Button(btns, text="Ajouter", command=self.add_emp).pack(side=tk.LEFT)
        ttk.Button(btns, text="Renommer", command=self.rename_emp).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Supprimer", command=self.del_emp).pack(side=tk.LEFT, padx=6)

    def refresh(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for r in self.db.employees():
            self.tree.insert('', 'end', iid=r['id'], values=(r['name'],))

    def add_emp(self):
        name = simpledialog.askstring("Ajouter", "Nom de l'employé :", parent=self)
        if name:
            try:
                self.db.add_employee(name.strip())
                self.refresh()
            except sqlite3.IntegrityError:
                messagebox.showerror("Erreur", "Nom déjà existant")

    def rename_emp(self):
        sel = self.tree.selection()
        if not sel:
            return
        emp_id = int(sel[0])
        current = self.tree.item(sel[0], 'values')[0]
        new_name = simpledialog.askstring("Renommer", "Nouveau nom :", initialvalue=current, parent=self)
        if new_name and new_name.strip() and new_name.strip() != current:
            try:
                self.db.rename_employee(emp_id, new_name.strip())
                self.refresh()
            except sqlite3.IntegrityError:
                messagebox.showerror("Erreur", "Nom déjà existant")

    def del_emp(self):
        sel = self.tree.selection()
        if not sel:
            return
        emp_id = int(sel[0])
        if messagebox.askyesno("Confirmer", "Supprimer cet employé ?"):
            self.db.delete_employee(emp_id)
            self.refresh()

# ---------- Présence (tableau aligné, sans clignotement) ----------
class PresenceWin(tk.Toplevel):
    def __init__(self, master, db: DB):
        tk.Toplevel.__init__(self, master)
        self.db = db
        self.title("Présence journalière")
        self.geometry("1100x680")
        self.current_date = today_local()
        self._last_can_activate = None
        self.build()
        self.load_day()
        self.after(1000, self.tick)

    def build(self):
        head = ttk.Frame(self)
        head.pack(fill=tk.X, padx=12, pady=8)
        self.lbl_dt = ttk.Label(head, text="", font=("Segoe UI", 12, "bold"))
        self.lbl_dt.pack(side=tk.LEFT)
        ttk.Button(head, text="Choisir une date", command=self.choose_date).pack(side=tk.RIGHT)

        ctrl = ttk.Frame(self)
        ctrl.pack(fill=tk.X, padx=12, pady=(0,8))
        self.btn_halfday = ttk.Button(ctrl, text="Activer Demi‑journée", command=self.apply_halfday)
        self.btn_halfday.pack(side=tk.LEFT)

        # Conteneur scrollable
        container = ttk.Frame(self)
        container.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)
        self.canvas = tk.Canvas(container, borderwidth=0, highlightthickness=0)
        vsb = ttk.Scrollbar(container, orient="vertical", command=self.canvas.yview)
        self.rows_frame = ttk.Frame(self.canvas)
        self.rows_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0,0), window=self.rows_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=vsb.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Mise en page tableau (4 colonnes uniformes)
        for i, text in enumerate(["Employé", "Matin", "Après‑midi", "Statut"]):
            ttk.Label(self.rows_frame, text=text, font=("Segoe UI", 10, "bold")).grid(row=0, column=i, padx=8, pady=6, sticky='w')
            self.rows_frame.grid_columnconfigure(i, weight=1, uniform="cols")

    def tick(self):
        now = now_local()
        self.lbl_dt.config(text="{}  {}".format(self.current_date.strftime(DATE_FMT), now.strftime("%H:%M:%S")))
        # Bouton Demi‑journée activable seulement après le pivot et si non appliqué
        pivot = self.db.get("halfday_pivot", DEFAULT_SETTINGS["halfday_pivot"]) or "10:00"
        can_activate = (now.time() >= datetime.strptime(pivot, "%H:%M").time()) and (self.db.get_halfday_flag(self.current_date) == 0)
        if can_activate != self._last_can_activate:
            self.btn_halfday['state'] = tk.NORMAL if can_activate else tk.DISABLED
            self._last_can_activate = can_activate
        self.after(1000, self.tick)

    def load_day(self):
        self.db.ensure_day(self.current_date)
        self.render_rows()

    def render_rows(self):
        # Efface lignes existantes (sauf en-tête)
        for w in list(self.rows_frame.grid_slaves()):
            info = w.grid_info()
            if int(info.get('row', 1)) > 0:
                w.destroy()

        rows = self.db.day_rows(self.current_date)
        arrival = self.db.get("arrival_time", DEFAULT_SETTINGS["arrival_time"]) or "08:00"
        arrival_t = datetime.strptime(arrival, "%H:%M").time()
        show_pm = (self.db.get_halfday_flag(self.current_date) == 1)  # PM visible uniquement après activation

        for idx, r in enumerate(rows, start=1):
            # Col 0: Nom
            ttk.Label(self.rows_frame, text=r['name']).grid(row=idx, column=0, padx=8, pady=4, sticky='w')

            # Col 1: Matin (deux boutons indépendants)
            am_frame = ttk.Frame(self.rows_frame)
            am_frame.grid(row=idx, column=1, padx=8, pady=4, sticky='w')
            ttk.Button(am_frame, text="Présent", command=self._mk_am_present(r, arrival_t)).pack(side=tk.LEFT)
            ttk.Button(am_frame, text="Absent", command=self._mk_am_absent(r)).pack(side=tk.LEFT, padx=(12,0))

            # Col 2: Après‑midi (visible seulement si demi‑journée activée)
            pm_frame = ttk.Frame(self.rows_frame)
            pm_frame.grid(row=idx, column=2, padx=8, pady=4, sticky='w')
            if show_pm:
                if r['am_status'] == 'absent':
                    ttk.Button(pm_frame, text="Présent (PM)", command=self._mk_pm_present_after_am_abs(r)).pack(side=tk.LEFT)
                if r['am_status'] in ('present','late'):
                    ttk.Button(pm_frame, text="Absent (PM)", command=self._mk_pm_absent_after_am_pres(r)).pack(side=tk.LEFT)

            # Col 3: Statut
            statut_txt, color = self.compute_status_text(r)
            lbl = ttk.Label(self.rows_frame, text=statut_txt)
            try:
                lbl.configure(foreground=color)
            except Exception:
                pass
            lbl.grid(row=idx, column=3, padx=8, pady=4, sticky='w')

    # Créateurs de callbacks (évitent le bug "tout le monde change")
    def _mk_am_present(self, row: sqlite3.Row, arrival_t: time):
        def _cb():
            nowdt = now_local()
            status = 'present'
            if nowdt.time() > arrival_t:
                confirm_late = messagebox.askyesno(
                    "Confirmation retard",
                    (
                        f"Il est {nowdt.strftime('%H:%M')}, soit après l'heure d'arrivée prévue "
                        f"({arrival_t.strftime('%H:%M')}).\n"
                        f"{row['name']} sera compté comme en retard. Confirmez-vous ?"
                    ),
                )
                if confirm_late:
                    status = 'late'
            self.db.set_am(row['employee_id'], self.current_date, status, nowdt)
            self.render_rows()
        return _cb

    def _mk_am_absent(self, row: sqlite3.Row):
        def _cb():
            self.db.set_am(row['employee_id'], self.current_date, 'absent', None)
            self.render_rows()
        return _cb

    def _mk_pm_present_after_am_abs(self, row: sqlite3.Row):
        def _cb():
            self.db.set_pm(row['employee_id'], self.current_date, 'present', now_local())
            self.render_rows()
        return _cb

    def _mk_pm_absent_after_am_pres(self, row: sqlite3.Row):
        def _cb():
            self.db.set_pm(row['employee_id'], self.current_date, 'absent', None)
            self.render_rows()
        return _cb

    def compute_status_text(self, r: sqlite3.Row) -> Tuple[str, str]:
        if r['am_status'] in ('present', 'late') and (not r['pm_status'] or r['pm_status'] == 'none'):
            return ("En retard", 'orange') if r['am_status'] == 'late' else ("Présent", 'green')
        if r['am_status'] == 'absent' and (not r['pm_status'] or r['pm_status'] == 'none'):
            return ("Absent", 'red')
        if r['am_status'] == 'absent' and r['pm_status'] == 'present':
            return ("½ journée d'absence", 'red')
        if r['am_status'] in ('present','late') and r['pm_status'] == 'absent':
            return ("½ journée d'absence", 'red')
        if r['am_status'] in ('present','late') and r['pm_status'] in ('present','late'):
            return ("Présent", 'green')
        if r['am_status'] == 'absent' and r['pm_status'] == 'absent':
            return ("Absent", 'red')
        return ("—", 'black')

    def apply_halfday(self):
        if self.db.get_halfday_flag(self.current_date):
            return
        rows = self.db.day_rows(self.current_date)
        for r in rows:
            if r['am_status'] == 'late':
                self.db.set_am(r['employee_id'], self.current_date, 'present', now_local())
        self.db.set_halfday_flag(self.current_date, 1)
        self.render_rows()
        # Met à jour l'état du bouton
        self.btn_halfday['state'] = tk.DISABLED

    def choose_date(self):
        if not check_admin(self, self.db):
            return
        s = simpledialog.askstring("Choisir une date", "Date (YYYY-MM-DD):", parent=self)
        d = parse_date(s) if s else None
        if not d:
            return
        self.current_date = d
        self.load_day()

# ---------- Paramètres ----------
class SettingsWin(tk.Toplevel):
    DAYS = [("Lun", "Mon"), ("Mar", "Tue"), ("Mer", "Wed"), ("Jeu", "Thu"), ("Ven", "Fri"), ("Sam", "Sat"), ("Dim", "Sun")]

    def __init__(self, master, db: DB):
        tk.Toplevel.__init__(self, master)
        self.db = db
        self.title("Paramètres")
        self.geometry("860x660")
        self.build()

    def build(self):
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True)

        self.tab_work = ttk.Frame(nb)
        self.tab_holidays = ttk.Frame(nb)
        self.tab_report = ttk.Frame(nb)
        self.tab_admin = ttk.Frame(nb)

        nb.add(self.tab_work, text="Jours & Heures")
        nb.add(self.tab_holidays, text="Jours fériés")
        nb.add(self.tab_report, text="Bilan du mois")
        nb.add(self.tab_admin, text="Admin")

        # ----- Jours & Heures -----
        frm = ttk.Frame(self.tab_work, padding=14)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frm, text="Jours travaillés", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, columnspan=7, sticky='w', pady=(0,6))
        days_grid = ttk.Frame(frm)
        days_grid.grid(row=1, column=0, columnspan=7, sticky='ew')
        for i in range(7):
            days_grid.grid_columnconfigure(i, weight=1, uniform="days")
        self.day_vars = {}
        current = set((self.db.get("workdays", DEFAULT_SETTINGS["workdays"]) or "Mon,Tue,Wed,Thu,Fri").split(','))
        for i, (label, key) in enumerate(self.DAYS):
            var = tk.BooleanVar(value=(key in current))
            self.day_vars[key] = var
            pad = (14,6) if i == 0 else 6  # petit décalage homogène pour **Lundi**
            ttk.Checkbutton(days_grid, text=label, variable=var).grid(row=0, column=i, padx=pad, pady=4, sticky='w')

        ttk.Label(frm, text="Heure d'arrivée (retard si après)").grid(row=2, column=0, sticky='w', pady=(16,4))
        self.arrival_var = tk.StringVar(value=self.db.get("arrival_time", DEFAULT_SETTINGS["arrival_time"]))
        ttk.Entry(frm, textvariable=self.arrival_var, width=8).grid(row=2, column=1, sticky='w')

        ttk.Label(frm, text="Heure pivot demi‑journée (affichage bouton)").grid(row=3, column=0, sticky='w', pady=(8,4))
        self.pivot_var = tk.StringVar(value=self.db.get("halfday_pivot", DEFAULT_SETTINGS["halfday_pivot"]))
        ttk.Entry(frm, textvariable=self.pivot_var, width=8).grid(row=3, column=1, sticky='w')

        ttk.Button(frm, text="Enregistrer", command=self.save_work_settings).grid(row=4, column=0, pady=16, sticky='w')

        # ----- Jours fériés -----
        hfrm = ttk.Frame(self.tab_holidays, padding=14)
        hfrm.pack(fill=tk.BOTH, expand=True)
        ttk.Button(hfrm, text="Récupérer en ligne (Madagascar)", command=self.fetch_holidays_online).pack(anchor='w')

        ttk.Label(hfrm, text="Ajouter manuellement", font=("Segoe UI", 10, "bold")).pack(anchor='w', pady=(12,4))
        addrow = ttk.Frame(hfrm)
        addrow.pack(fill=tk.X)
        ttk.Label(addrow, text="Date (YYYY-MM-DD)").pack(side=tk.LEFT)
        self.hday_var = tk.StringVar()
        ttk.Entry(addrow, textvariable=self.hday_var, width=12).pack(side=tk.LEFT, padx=(6,12))
        ttk.Label(addrow, text="Nom du jour férié").pack(side=tk.LEFT)
        self.hlabel_var = tk.StringVar()
        ttk.Entry(addrow, textvariable=self.hlabel_var, width=30).pack(side=tk.LEFT, padx=(6,12))
        self.is_working_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(addrow, text="Jour travaillé", variable=self.is_working_var).pack(side=tk.LEFT, padx=6)
        ttk.Button(addrow, text="Ajouter", command=self.add_manual_holiday).pack(side=tk.LEFT, padx=6)

        self.h_tree = ttk.Treeview(hfrm, columns=("date","label","travail"), show="headings", height=12)
        for c,w in [("date",140),("label",420),("travail",140)]:
            self.h_tree.heading(c, text=c.capitalize())
            self.h_tree.column(c, width=w, anchor='w')
        self.h_tree.pack(fill=tk.BOTH, expand=True, pady=8)
        ttk.Button(hfrm, text="Supprimer sélection", command=self.del_holiday).pack(anchor='w')
        self.refresh_holidays()

        # ----- Bilan du mois -----
        rpt = ttk.Frame(self.tab_report, padding=14)
        rpt.pack(fill=tk.BOTH, expand=True)
        self.rpt_tree = ttk.Treeview(rpt, columns=("employe","absences","retards"), show='headings', height=18)
        for c,w in [("employe",320),("absences",140),("retards",140)]:
            self.rpt_tree.heading(c, text=c.capitalize())
            self.rpt_tree.column(c, width=w, anchor='w')
        self.rpt_tree.pack(fill=tk.BOTH, expand=True)

        btns = ttk.Frame(rpt)
        btns.pack(fill=tk.X, pady=8)
        ttk.Button(btns, text="Actualiser (mois courant)", command=self.refresh_report).pack(side=tk.LEFT)
        ttk.Button(btns, text="Exporter CSV", command=self.export_month_csv).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Exporter Excel", command=self.export_month_xlsx).pack(side=tk.RIGHT, padx=6)

        self.lbl_summary = ttk.Label(rpt, text="")
        self.lbl_summary.pack(fill=tk.X, pady=(6,0))

        # ----- Admin -----
        adm = ttk.Frame(self.tab_admin, padding=14)
        adm.pack(fill=tk.BOTH, expand=True)
        ttk.Label(adm, text="Définir / changer le mot de passe admin").pack(anchor='w')
        ttk.Button(adm, text="Définir le mot de passe", command=self.set_admin_password).pack(anchor='w', pady=6)

    def save_work_settings(self):
        days = [k for (label,k) in self.DAYS if self.day_vars[k].get()]
        self.db.set("workdays", ",".join(days))
        self.db.set("arrival_time", self.arrival_var.get().strip() or DEFAULT_SETTINGS["arrival_time"])
        self.db.set("halfday_pivot", self.pivot_var.get().strip() or DEFAULT_SETTINGS["halfday_pivot"])
        messagebox.showinfo("OK", "Paramètres enregistrés")

    def refresh_holidays(self):
        for i in self.h_tree.get_children():
            self.h_tree.delete(i)
        for r in self.db.list_holidays():
            self.h_tree.insert('', 'end', iid=r['date'], values=(r['date'], r['label'] or '', 'Oui' if r['is_working_day'] else 'Non'))

    def add_manual_holiday(self):
        d = parse_date(self.hday_var.get().strip())
        if not d:
            messagebox.showerror("Erreur", "Date invalide (YYYY-MM-DD)")
            return
        label = self.hlabel_var.get().strip() or "Férié"
        is_work = self.is_working_var.get()
        if any(k in label.lower() for k in ("eid","aïd","aid")):
            is_work = True
        self.db.add_holiday(d, label, is_work)
        self.refresh_holidays()
        self.hday_var.set("")
        self.hlabel_var.set("")

    def del_holiday(self):
        sel = self.h_tree.selection()
        if not sel:
            return
        d = parse_date(sel[0])
        if d:
            self.db.remove_holiday(d)
            self.refresh_holidays()

    def _ssl_context(self):
        try:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ctx = ssl.create_default_context()
        return ctx

    def fetch_holidays_online(self):
        year = today_local().year
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/MG"
        try:
            req = Request(url, headers={"User-Agent": "AtelierAbsences/1.1"})
            with urlopen(req, context=self._ssl_context(), timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            for item in data:
                d = parse_date(item.get('date'))
                if not d:
                    continue
                name = item.get('localName') or item.get('name') or 'Férié'
                is_work = 0
                low = (name or '').lower()
                if any(k in low for k in ("eid","aïd","aid")):
                    is_work = 1
                self.db.add_holiday(d, name, bool(is_work))
            self.refresh_holidays()
            messagebox.showinfo("OK", f"Jours fériés {year} importés.")
        except (HTTPError, URLError, ssl.SSLError) as e:
            messagebox.showerror("Erreur", f"Échec de récupération en ligne. Détail: {e}\nVous pouvez ajouter manuellement les fériés ci‑dessous.")

    # ----- Bilan du mois -----
    def month_bounds(self, ref: date) -> Tuple[date, date]:
        start = ref.replace(day=1)
        if start.month == 12:
            end = start.replace(year=start.year+1, month=1, day=1) - timedelta(days=1)
        else:
            end = start.replace(month=start.month+1, day=1) - timedelta(days=1)
        return start, end

    def refresh_report(self):
        for i in self.rpt_tree.get_children():
            self.rpt_tree.delete(i)
        start, end = self.month_bounds(today_local())
        rows = self.compute_month_stats(start, end)
        for name, a_abs, a_ret in rows:
            self.rpt_tree.insert('', 'end', values=(name, a_abs, a_ret))
        if rows:
            max_abs = max(rows, key=lambda x: x[1])[1]
            most_abs = [r[0] for r in rows if r[1] == max_abs and max_abs > 0]
            max_ret = max(rows, key=lambda x: x[2])[2]
            most_ret = [r[0] for r in rows if r[2] == max_ret and max_ret > 0]
            none_abs = [r[0] for r in rows if r[1] == 0]
            total_abs = sum(r[1] for r in rows)
            parts = []
            if most_abs:
                parts.append("Le(s) plus absent(s) : " + ", ".join(most_abs))
            if most_ret:
                parts.append("Le(s) plus en retard : " + ", ".join(most_ret))
            if none_abs:
                parts.append("Aucune absence : " + ", ".join(none_abs))
            parts.append("Total absences : {:.1f}".format(total_abs))
            self.lbl_summary.config(text=" | ".join(parts))
        else:
            self.lbl_summary.config(text="")

    def is_working_day(self, d: date) -> bool:
        wd = d.strftime("%a")
        workset = set((self.db.get("workdays", DEFAULT_SETTINGS["workdays"]) or "Mon,Tue,Wed,Thu,Fri").split(','))
        if wd not in workset:
            return False
        r = self.db.conn.execute("SELECT is_working_day FROM holidays WHERE date=?", (d.strftime(DATE_FMT),)).fetchone()
        if r is not None:
            return bool(r[0])
        return True

    def compute_month_stats(self, start: date, end: date) -> List[Tuple[str, float, float]]:
        emps = {r['id']: r['name'] for r in self.db.employees()}
        stats = {eid: [emps[eid], 0.0, 0] for eid in emps}
        cur = self.db.conn.execute(
            "SELECT * FROM attendance WHERE date BETWEEN ? AND ?",
            (start.strftime(DATE_FMT), end.strftime(DATE_FMT)),
        )
        for r in cur:
            d = datetime.strptime(r['date'], DATE_FMT).date()
            if self.is_holiday_off(d):
                continue
            wd = d.weekday()
            if wd == 6:  # dimanche ignoré
                continue
            half_unit = 0.5 if wd <= 4 else 0.25  # samedi = demi-journée travaillée
            am = (r['am_status'] or 'none')
            pm = (r['pm_status'] or 'none')
            rec = stats[r['employee_id']]
            if pm == 'none':
                if am == 'absent':
                    rec[1] += half_unit * 2
                elif am == 'late':
                    rec[2] += 1
            else:
                if am == 'absent' and pm in ('present', 'late'):
                    rec[1] += half_unit
                if am in ('present', 'late') and pm == 'absent':
                    rec[1] += half_unit
                if am == 'absent' and pm == 'absent':
                    rec[1] += half_unit * 2
                if am == 'late':
                    rec[2] += 1
        return list(stats.values())

    def is_holiday_off(self, d: date) -> bool:
        row = self.db.conn.execute(
            "SELECT is_working_day FROM holidays WHERE date=?",
            (d.strftime(DATE_FMT),),
        ).fetchone()
        if row is not None:
            return int(row[0]) == 0
        return False

    def export_month_csv(self):
        self.refresh_report()
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV",".csv")])
        if not path:
            return
        rows = [self.rpt_tree.item(i, 'values') for i in self.rpt_tree.get_children()]
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["Employé","Absences","Retards"])
            for r in rows:
                w.writerow(r)
        messagebox.showinfo("OK", "Export CSV terminé")

    def export_month_xlsx(self):
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font
        except Exception:
            messagebox.showerror("Manquant", "openpyxl n'est pas installé. Faites: pip install openpyxl")
            return
        self.refresh_report()
        path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel",".xlsx")])
        if not path:
            return
        wb = Workbook()
        ws = wb.active
        ws.title = "Bilan du mois"
        ws.append(["Employé","Absences","Retards"])
        for iid in self.rpt_tree.get_children():
            ws.append(list(self.rpt_tree.item(iid,'values')))
        for c in ws[1]:
            c.font = Font(bold=True)
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        wb.save(path)
        messagebox.showinfo("OK", "Export Excel terminé")

    def set_admin_password(self):
        if not check_admin(self, self.db, ask_if_empty=False):
            return
        new = simpledialog.askstring("Mot de passe", "Nouveau mot de passe :", parent=self, show='*')
        if new is None:
            return
        self.db.set("admin_password", new)
        messagebox.showinfo("OK", "Mot de passe mis à jour")

# ---------- Salaires ----------
class SalariesWin(tk.Toplevel):
    def __init__(self, master, db: DB):
        tk.Toplevel.__init__(self, master)
        self.db = db
        self.title("Gestion des salaires")
        self.geometry("1060x680")
        self.current_month = today_local().replace(day=1)
        self.salary_tab_built = False
        self.salary_admin_granted = False
        self.detail_windows = {}
        self.build()

    def build(self):
        container = ttk.Frame(self, padding=12)
        container.pack(fill=tk.BOTH, expand=True)
        ttk.Label(container, text="Gestion salariale", font=("Segoe UI", 20, "bold")).pack(anchor='w', pady=(0, 12))

        self.notebook = ttk.Notebook(container)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.tab_advances = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_advances, text="Avances")
        self.tab_salary = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_salary, text="Bilan salarial")
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)

        self.build_advances_tab()
        self.refresh_advances_tab()

    # ----- Avances -----
    def build_advances_tab(self):
        frame = ttk.Frame(self.tab_advances, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Avances par employé", font=("Segoe UI", 14, "bold")).pack(anchor='w')
        ttk.Label(
            frame,
            text="Double-cliquez sur un employé pour voir le détail des mouvements.",
            foreground="gray",
        ).pack(anchor='w', pady=(2, 10))

        columns = ("employee", "total")
        self.adv_tree = ttk.Treeview(frame, columns=columns, show="headings", height=18)
        self.adv_tree.heading("employee", text="Employé")
        self.adv_tree.heading("total", text="Total des avances")
        self.adv_tree.column("employee", width=320, anchor='w')
        self.adv_tree.column("total", width=160, anchor='e')
        self.adv_tree.pack(fill=tk.BOTH, expand=True)
        self.adv_tree.bind("<Double-1>", self.on_advances_double_click)

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X, pady=8)
        ttk.Button(btns, text="Rafraîchir", command=self.refresh_advances_tab).pack(side=tk.LEFT)

        self.adv_summary = ttk.Label(frame, text="")
        self.adv_summary.pack(anchor='w', pady=(6, 0))

    def refresh_advances_tab(self):
        for item in self.adv_tree.get_children():
            self.adv_tree.delete(item)
        today = today_local()
        rows = self.db.monthly_advances(today.year, today.month)
        total_amount = 0.0
        for r in rows:
            total = float(r["total"] or 0)
            total_amount += total
            self.adv_tree.insert('', 'end', iid=str(r["employee_id"]), values=(r["name"], f"{total:.2f}"))
        self.adv_summary.config(
            text="Total des avances du mois en cours : {:.2f}".format(total_amount)
        )

    def on_advances_double_click(self, event):
        item = self.adv_tree.identify_row(event.y)
        if not item:
            return
        emp_id = int(item)
        emp_name = self.adv_tree.set(item, "employee")
        self.open_advances_detail(emp_id, emp_name)

    def open_advances_detail(self, emp_id: int, emp_name: str):
        existing = self.detail_windows.get(emp_id)
        if existing and existing.winfo_exists():
            existing.focus_force()
            existing.lift()
            return

        win = AdvanceDetailsWin(
            self,
            self.db,
            emp_id,
            emp_name,
            on_change=self.refresh_after_advances,
        )

        def _on_close():
            self.detail_windows.pop(emp_id, None)
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", _on_close)
        self.detail_windows[emp_id] = win

    def refresh_after_advances(self):
        self.refresh_advances_tab()
        if self.salary_tab_built:
            self.refresh_salary_tab()

    # ----- Bilan salarial -----
    def on_tab_changed(self, event):
        tab_id = event.widget.select()
        if tab_id == str(self.tab_salary):
            if not self.salary_admin_granted:
                if not check_admin(self, self.db):
                    self.after(10, lambda: self.notebook.select(self.tab_advances))
                    return
                self.salary_admin_granted = True
            if not self.salary_tab_built:
                self.build_salary_tab()
            self.refresh_salary_tab()

    def build_salary_tab(self):
        self.salary_tab_built = True
        frame = ttk.Frame(self.tab_salary, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(frame)
        header.pack(fill=tk.X)
        ttk.Label(header, text="Bilan salarial du mois", font=("Segoe UI", 14, "bold")).pack(side=tk.LEFT)
        self.month_label = ttk.Label(header, text="")
        self.month_label.pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(header, text="Actualiser", command=self.refresh_salary_tab).pack(side=tk.RIGHT)

        columns = (
            "employee",
            "base",
            "abs_count",
            "abs_amount",
            "late_count",
            "late_amount",
            "malus",
            "bonus",
            "advances",
            "net",
        )
        self.salary_tree = ttk.Treeview(frame, columns=columns, show="headings", height=18)
        headings = {
            "employee": "Employé",
            "base": "Base",
            "abs_count": "Absences (#)",
            "abs_amount": "Absences (montant)",
            "late_count": "Retards (#)",
            "late_amount": "Retards (montant)",
            "malus": "Malus",
            "bonus": "Bonus",
            "advances": "Avances",
            "net": "Salaire net",
        }
        for col in columns:
            self.salary_tree.heading(col, text=headings[col])
            anchor = 'w' if col in ("employee",) else 'e'
            width = 240 if col == "employee" else 120
            self.salary_tree.column(col, anchor=anchor, width=width, minwidth=100)
        self.salary_tree.pack(fill=tk.BOTH, expand=True, pady=8)
        self.salary_tree.bind("<Double-1>", self.on_salary_double_click)

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Définir Base", command=lambda: self.prompt_profile_value("base")).pack(side=tk.LEFT)
        ttk.Button(btns, text="Définir Malus", command=lambda: self.prompt_profile_value("malus")).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Définir Bonus", command=lambda: self.prompt_profile_value("bonus")).pack(side=tk.LEFT)

        self.salary_summary = ttk.Label(frame, text="")
        self.salary_summary.pack(anchor='w', pady=(6, 0))

    def refresh_salary_tab(self):
        if not self.salary_tab_built:
            return
        month_name = self.current_month.strftime("%B %Y")
        self.month_label.config(text=month_name.capitalize())

        for item in self.salary_tree.get_children():
            self.salary_tree.delete(item)

        employees = self.db.employees()
        profiles = self.db.salary_profiles_map()
        stats = self.collect_month_metrics()
        year = self.current_month.year
        month = self.current_month.month
        adv_rows = {r["employee_id"]: float(r["total"] or 0.0) for r in self.db.monthly_advances(year, month)}
        work_units = self.compute_work_units()
        late_penalty = self.get_late_penalty_amount()
        self.salary_tree.heading("late_amount", text=f"Retards (montant @ {late_penalty:.2f})")

        total_net = 0.0
        for emp in employees:
            emp_id = emp["id"]
            profile = profiles.get(emp_id)
            base = float((profile["base_salary"] if profile else 0.0) or 0.0)
            bonus = float((profile["bonus"] if profile else 0.0) or 0.0)
            malus = float((profile["malus"] if profile else 0.0) or 0.0)
            metric = stats.get(emp_id, {"abs_units": 0.0, "late": 0})
            abs_units = float(metric["abs_units"])
            absence_amount = 0.0
            if base > 0 and work_units > 0:
                absence_amount = (base / work_units) * abs_units
            late_count = int(metric["late"])
            late_amount = late_count * late_penalty
            advances = adv_rows.get(emp_id, 0.0)
            net = base - absence_amount - late_amount - malus + bonus - advances
            total_net += net

            self.salary_tree.insert(
                '',
                'end',
                iid=str(emp_id),
                values=(
                    emp["name"],
                    f"{base:.2f}",
                    f"{abs_units:.2f}",
                    f"{absence_amount:.2f}",
                    str(late_count),
                    f"{late_amount:.2f}",
                    f"{malus:.2f}",
                    f"{bonus:.2f}",
                    f"{advances:.2f}",
                    f"{net:.2f}",
                ),
            )

        summary = (
            f"Total net à verser : {total_net:.2f} | "
            f"Unité(s) de travail du mois : {work_units:.2f} | "
            f"Tarif retard : {late_penalty:.2f}"
        )
        self.salary_summary.config(text=summary)

    def on_salary_double_click(self, event):
        region = self.salary_tree.identify_region(event.x, event.y)
        if region == "heading":
            column = self.salary_tree.identify_column(event.x)
            try:
                col_index = int(column.lstrip('#')) - 1
            except ValueError:
                return
            col_name = self.salary_tree["columns"][col_index]
            if col_name == "late_amount":
                self.prompt_late_penalty()
            return
        if region != "cell":
            return
        column = self.salary_tree.identify_column(event.x)
        try:
            col_index = int(column.lstrip('#')) - 1
        except ValueError:
            return
        col_name = self.salary_tree["columns"][col_index]
        if col_name in ("base", "malus", "bonus"):
            item = self.salary_tree.identify_row(event.y)
            if item:
                self.prompt_profile_value(col_name, item=item)

    def selected_employee(self) -> Optional[str]:
        sel = self.salary_tree.selection()
        if not sel:
            messagebox.showinfo("Sélection", "Sélectionnez un employé.")
            return None
        return sel[0]

    def prompt_profile_value(self, column: str, item: Optional[str] = None):
        if not self.salary_tab_built:
            return
        if item is None:
            item = self.selected_employee()
        if not item:
            return
        emp_id = int(item)
        emp_name = self.salary_tree.set(item, "employee")
        current_value = self.salary_tree.set(item, column) or "0"
        try:
            initial = float(current_value.replace(',', '.'))
        except ValueError:
            initial = 0.0
        labels = {
            "base": "Salaire de base",
            "malus": "Malus",
            "bonus": "Bonus",
        }
        value = simpledialog.askfloat(
            labels[column],
            f"Valeur pour {emp_name} :",
            parent=self,
            initialvalue=initial,
        )
        if value is None:
            return
        field_map = {
            "base": "base_salary",
            "malus": "malus",
            "bonus": "bonus",
        }
        self.db.update_salary_profile(emp_id, **{field_map[column]: value})
        self.refresh_salary_tab()

    def prompt_late_penalty(self):
        current = self.get_late_penalty_amount()
        value = simpledialog.askfloat(
            "Tarif retard",
            "Montant retenu par retard :",
            parent=self,
            initialvalue=current,
            minvalue=0.0,
        )
        if value is None:
            return
        self.db.set("late_penalty_amount", f"{value:.2f}")
        self.refresh_salary_tab()

    def get_late_penalty_amount(self) -> float:
        try:
            return float(self.db.get("late_penalty_amount", "0") or 0.0)
        except ValueError:
            return 0.0

    def month_limits(self) -> Tuple[date, date]:
        start = self.current_month
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = start.replace(month=start.month + 1, day=1) - timedelta(days=1)
        return start, end

    def compute_work_units(self) -> float:
        start, end = self.month_limits()
        total = 0.0
        d = start
        while d <= end:
            if self.is_holiday_off(d):
                d += timedelta(days=1)
                continue
            wd = d.weekday()
            if wd <= 4:
                total += 1.0
            elif wd == 5:
                total += 0.5
            d += timedelta(days=1)
        return total

    def is_holiday_off(self, d: date) -> bool:
        row = self.db.conn.execute(
            "SELECT is_working_day FROM holidays WHERE date=?",
            (d.strftime(DATE_FMT),),
        ).fetchone()
        if row is not None:
            return int(row[0]) == 0
        return False

    def collect_month_metrics(self) -> dict:
        start, end = self.month_limits()
        stats = {r["id"]: {"abs_units": 0.0, "late": 0} for r in self.db.employees()}
        cur = self.db.conn.execute(
            "SELECT * FROM attendance WHERE date BETWEEN ? AND ?",
            (start.strftime(DATE_FMT), end.strftime(DATE_FMT)),
        )
        for row in cur:
            d = datetime.strptime(row["date"], DATE_FMT).date()
            if self.is_holiday_off(d):
                continue
            wd = d.weekday()
            if wd == 6:
                continue
            half_unit = 0.5 if wd <= 4 else 0.25
            am = row["am_status"] or "none"
            pm = row["pm_status"] or "none"
            emp_id = row["employee_id"]
            if pm == "none":
                if am == "absent":
                    stats[emp_id]["abs_units"] += half_unit * 2
                elif am == "late":
                    stats[emp_id]["late"] += 1
            else:
                if am == "absent" and pm in ("present", "late"):
                    stats[emp_id]["abs_units"] += half_unit
                if am in ("present", "late") and pm == "absent":
                    stats[emp_id]["abs_units"] += half_unit
                if am == "absent" and pm == "absent":
                    stats[emp_id]["abs_units"] += half_unit * 2
                if am == "late":
                    stats[emp_id]["late"] += 1
        return stats


class AdvanceDetailsWin(tk.Toplevel):
    def __init__(self, master, db: DB, employee_id: int, employee_name: str, on_change=None):
        tk.Toplevel.__init__(self, master)
        self.db = db
        self.employee_id = employee_id
        self.employee_name = employee_name
        self.on_change = on_change or (lambda: None)
        self.title(f"Avances — {employee_name}")
        self.geometry("600x460")
        self.build()
        self.refresh()

    def build(self):
        frame = ttk.Frame(self, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(frame, text=f"Avances pour {self.employee_name}", font=("Segoe UI", 12, "bold")).pack(anchor='w')

        columns = ("taken_at", "amount", "comment")
        self.tree = ttk.Treeview(frame, columns=columns, show="headings", height=14)
        self.tree.heading("taken_at", text="Date")
        self.tree.heading("amount", text="Montant")
        self.tree.heading("comment", text="Commentaire")
        self.tree.column("taken_at", width=170, anchor='w')
        self.tree.column("amount", width=100, anchor='e')
        self.tree.column("comment", width=260, anchor='w')
        self.tree.pack(fill=tk.BOTH, expand=True, pady=8)

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Ajouter", command=self.add_advance).pack(side=tk.LEFT)
        ttk.Button(btns, text="Supprimer", command=self.delete_selected).pack(side=tk.LEFT, padx=6)

        self.total_label = ttk.Label(frame, text="")
        self.total_label.pack(anchor='w', pady=(6, 0))

    def refresh(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        rows = self.db.advances_for_employee(self.employee_id)
        total = 0.0
        for r in rows:
            total += float(r["amount"] or 0.0)
            self.tree.insert(
                '',
                'end',
                iid=str(r["id"]),
                values=(r["taken_at"], f"{float(r['amount']):.2f}", r["comment"] or ""),
            )
        self.total_label.config(text=f"Total cumulé : {total:.2f}")

    def add_advance(self):
        amount = simpledialog.askfloat("Avance", "Montant de l'avance :", parent=self, minvalue=0.0)
        if amount is None:
            return
        comment = simpledialog.askstring("Commentaire", "Commentaire (optionnel) :", parent=self)
        comment = comment.strip() if comment else None
        self.db.add_advance(self.employee_id, amount, taken_at=datetime.now(), comment=comment)
        self.refresh()
        self.on_change()

    def delete_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        adv_id = int(sel[0])
        if not messagebox.askyesno("Confirmer", "Supprimer cette avance ?", parent=self):
            return
        self.db.delete_advance(adv_id)
        self.refresh()
        self.on_change()

# ---------- Sécurité simple ----------
def check_admin(parent, db: DB, ask_if_empty=True) -> bool:
    current = db.get("admin_password", "")
    if not current:
        if ask_if_empty:
            messagebox.showinfo("Info", "Aucun mot de passe admin défini (Paramètres > Admin)")
            return False
        return True
    pwd = simpledialog.askstring("Admin", "Mot de passe :", parent=parent, show='*')
    if pwd is None:
        return False
    return pwd == current

# -------------------- run --------------------
if __name__ == "__main__":
    App().mainloop()
