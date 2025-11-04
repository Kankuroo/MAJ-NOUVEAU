"""
Application de gestion de production pour une bijouterie spécialisée dans l'argent.

Cette application remplace un fichier Excel de suivi de production. Elle permet de créer et
gérer des comptes JBA (travaux), d'enregistrer les différentes étapes d'un travail avec
des horodatages verrouillés, de calculer les pertes de poids et la durée de travail en
heures ouvrables, et de générer des synthèses ou des exports.

L'interface est conçue pour rester simple : une fenêtre principale liste les comptes
existants et propose des actions pour en ajouter de nouveaux, les consulter ou les
exporter. Une fenêtre de détail permet de saisir les informations de chaque étape.

La base de données SQLite stocke toutes les données en local afin que le logiciel
fonctionne hors‑ligne. Un exemple de base de données (« sample_bijouterie.db ») peut
être généré via le script `create_sample_db.py`.

Pour démarrer l'application :

    python bijouterie_app.py

Pré-requis : Python 3.7+, dépendances standard (tkinter est inclus). Pour la génération
d'Excel, la bibliothèque pandas et openpyxl sont nécessaires (installables via pip).
"""

import ast
import time as systime
import time
import os
import sqlite3
import sys
import json
import shutil
import re
from typing import Optional, Sequence
from datetime import datetime, time, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import tkinter.font as tkfont

try:
    import pandas as pd
except ImportError:
    pd = None  # Optionnel, utilisé uniquement pour l'export Excel


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_CODE = "JBA"
DB_FILENAME = os.path.join(BASE_DIR, "bijouterie_JBA.db")
RESET_HISTORY_FILENAME = f"resets_{APP_CODE.lower()}.json"

CARATS_PER_GRAM = 5.0
GRAMS_PER_CARAT = 1.0 / CARATS_PER_GRAM

USE_API = os.environ.get("JBA_USE_API", "0").lower() in {"1", "true", "yes", "on"}
API_BASE = os.environ.get("JBA_API_BASE", "http://127.0.0.1:8000")

# -----------------------------------------------------------------------------
# Chargement d'un fichier .env (facultatif)
#
# Si un fichier nommé ``.env`` se trouve dans le même répertoire que ce script,
# chaque ligne de la forme ``VAR=valeur`` y sera chargée dans l'environnement
# avant d'utiliser ces variables au sein de l'application. Cela permet d'éviter
# de saisir manuellement les paramètres dans votre terminal : il suffit
# d'éditer le fichier .env avec vos valeurs.
try:
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key = key.strip()
                val = val.strip()
                # Ne pas écraser les variables déjà définies dans l'environnement
                if key and val and key not in os.environ:
                    os.environ[key] = val
except Exception:
    pass

# Police par défaut pour toute l'application
DEFAULT_FONT_FAMILY = "San Francisco"
FALLBACK_FONT_FAMILIES = (
    "Segoe UI",
    "Arial",
    "Helvetica",
    "TkDefaultFont",
)


STEP_COLOR_MAP = {
    'jobwork pret': '#00008B',
    'jobwork prêt': '#00008B',
    'tige': '#FF0000',
    'limage & montage': '#FFFF00',
    'limage/ montage': '#FFFF00',
    'limage/montage': '#FFFF00',
    'papier': '#FFA500',
    'sertissage': '#800080',
    'correction': '#FF69B4',
    'vérification qualité': '#A52A2A',
    'verification qualite': '#A52A2A',
    'polissage': '#1E90FF',
    'polissage final': '#00FFFF',
    'fini': '#008000',
}


def ensure_db_exists(db_path: str):
    """Crée la base de données et ses tables si elles n'existent pas déjà."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Création des tables si elles n'existent pas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref TEXT UNIQUE NOT NULL,
            description TEXT,
            status TEXT,
            start_time TEXT,
            end_time TEXT,
            total_loss REAL,
            total_loss_pct REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE COLLATE NOCASE,
            ordre INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            worker TEXT,
            issue_time TEXT,
            issue_desc TEXT,
            issue_gwt REAL,
            issue_scrap REAL,
            issue_stone REAL,
            issue_finding REAL,
            return_time TEXT,
            return_desc TEXT,
            return_gwt REAL,
            return_scrap REAL,
            return_stone REAL,
            return_finding REAL,
            loss_weight REAL,
            loss_pct REAL,
            position INTEGER,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
        """
    )
    # Détails pour Findings et Stone : type = 'findings' ou 'stone'
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            description TEXT,
            pcs INTEGER,
            gwt REAL,
            FOREIGN KEY(step_id) REFERENCES steps(id)
        )
        """
    )
    # Table des issues
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS issues(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id INTEGER NOT NULL,
            issue_time TEXT,
            gwt REAL,
            scrap REAL,
            stone REAL,
            finding REAL,
            description TEXT,
            gwt_formula TEXT,
            scrap_formula TEXT,
            stone_formula TEXT,
            finding_formula TEXT,
            FOREIGN KEY(step_id) REFERENCES steps(id)
        )
        """
    )
    # Table des livraisons
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deliveries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id INTEGER NOT NULL,
            delivery_time TEXT,
            gwt REAL,
            scrap REAL,
            return_stone REAL,
            return_finding REAL,
            description TEXT,
            gwt_formula TEXT,
            scrap_formula TEXT,
            return_stone_formula TEXT,
            return_finding_formula TEXT,
            FOREIGN KEY(step_id) REFERENCES steps(id)
        )
        """
    )
    # Ajout des colonnes manquantes pour compatibilité ascendante
    def ensure_columns(table_name, columns):
        cur.execute(f"PRAGMA table_info({table_name})")
        existing = {row[1] for row in cur.fetchall()}
        for name, definition in columns.items():
            if name not in existing:
                cur.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {name} {definition}"
                )

    ensure_columns(
        "issues",
        {
            "gwt_formula": "TEXT",
            "scrap_formula": "TEXT",
            "stone_formula": "TEXT",
            "finding_formula": "TEXT",
        },
    )
    ensure_columns(
        "deliveries",
        {
            "gwt_formula": "TEXT",
            "scrap_formula": "TEXT",
            "return_stone_formula": "TEXT",
            "return_finding_formula": "TEXT",
            "scrap_delivered": "INTEGER DEFAULT 0",
        },
    )
    ensure_columns(
        "steps",
        {
            "position": "INTEGER",
        },
    )

    # Initialise les positions manquantes en respectant l'ordre actuel (par id).
    cur.execute(
        "SELECT job_id, id FROM steps WHERE position IS NULL ORDER BY job_id, id"
    )
    rows = cur.fetchall()
    if rows:
        current_job = None
        next_position = 0
        for job_id, step_id in rows:
            if job_id != current_job:
                cur.execute(
                    "SELECT COALESCE(MAX(position), 0) FROM steps WHERE job_id=?",
                    (job_id,),
                )
                existing_max = cur.fetchone()[0] or 0
                next_position = existing_max
                current_job = job_id
            next_position += 1
            cur.execute(
                "UPDATE steps SET position=? WHERE id=?",
                (next_position, step_id),
            )

    # Normalise les valeurs numériques héritées stockées sous forme de texte.
    legacy_numeric_columns = {
        "steps": (
            "issue_gwt",
            "issue_scrap",
            "issue_stone",
            "issue_finding",
            "return_gwt",
            "return_scrap",
            "return_stone",
            "return_finding",
            "loss_weight",
            "loss_pct",
        ),
        "issues": ("gwt", "scrap", "stone", "finding"),
        "deliveries": ("gwt", "scrap", "return_stone", "return_finding"),
        "details": ("gwt",),
    }

    for table_name, columns in legacy_numeric_columns.items():
        if not columns:
            continue
        try:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
        except sqlite3.Error:
            continue
        if not cur.fetchone():
            continue
        columns_list = ", ".join(columns)
        cur.execute(f"SELECT id, {columns_list} FROM {table_name}")
        rows = cur.fetchall()
        for row in rows:
            row_id = row[0]
            updates = {}
            for idx, column in enumerate(columns, start=1):
                original = row[idx]
                normalized = normalize_numeric_value(original)
                if normalized is None:
                    if isinstance(original, str) and original.strip() == "":
                        updates[column] = None
                    continue
                if isinstance(original, (int, float)):
                    try:
                        if float(original) == float(normalized):
                            continue
                    except Exception:
                        pass
                updates[column] = normalized
            if updates:
                set_clause = ", ".join(f"{col}=?" for col in updates)
                values = list(updates.values())
                values.append(row_id)
                cur.execute(
                    f"UPDATE {table_name} SET {set_clause} WHERE id=?",
                    values,
                )

    conn.commit()
    conn.close()


def normalize_numeric_value(value):
    """Convertit des valeurs numériques legacy (texte) en float."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Supprime les espaces et séparateurs de milliers courants
        text = text.replace("\xa0", "").replace(" ", "")
        # Gestion des formats "1.234,56" ou "1,234.56"
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        else:
            text = text.replace(",", ".")
        try:
            return float(text)
        except ValueError:
            match = re.search(r"-?(?:\d+\.\d+|\d+|\.\d+)", text)
            if match:
                try:
                    return float(match.group())
                except ValueError:
                    return None
            return None


def grams_to_carats(value):
    """Convertit un poids en grammes vers les carats."""
    if value in (None, ""):
        return None
    try:
        return float(value) * CARATS_PER_GRAM
    except (TypeError, ValueError):
        return None


def carats_to_grams(value):
    """Convertit un poids en carats vers les grammes."""
    if value in (None, ""):
        return None
    try:
        return float(value) * GRAMS_PER_CARAT
    except (TypeError, ValueError):
        return None


def compute_loss(
    issue_gwt,
    issue_scrap,
    delivery_gwt_total,
    delivery_scrap_total,
    return_stone_total,
    return_finding_total,
    issue_finding=None,
    issue_stone=None,
):
    """Calcule la perte (g) et la perte (%) à partir des totaux agrégés.

    Les paramètres ``delivery_*`` et ``return_*`` doivent contenir les totaux
    additionnés de toutes les livraisons déjà saisies. La formule appliquée est
    la suivante :

        Perte (g) = GWT_ISSUE + Scrap_ISSUE - (GWT_DELIVERY - (Finding_ISSUE +
        Stone_ISSUE - Return_Stone - Return_Findings) + Scrap_DELIVERY)

        Perte (%) = Perte (g) / (GWT_DELIVERY - (Finding_ISSUE + Stone_ISSUE -
        Return_Stone - Return_Findings)) × 100

    Toutes les valeurs manquantes sont considérées comme ``0``.
    """
    # Convertit toutes les entrées en float, NaN->0
    def to_float(val):
        try:
            if val is None:
                return 0.0
            # test NaN
            if isinstance(val, float) and val != val:
                return 0.0
            return float(val)
        except Exception:
            return 0.0

    igwt = to_float(issue_gwt)
    iscrap = to_float(issue_scrap)
    rgwt = to_float(delivery_gwt_total)
    rscrap = to_float(delivery_scrap_total)
    rstone = to_float(return_stone_total)
    rfind = to_float(return_finding_total)
    ifind = to_float(issue_finding)
    istone = to_float(issue_stone)
    # Calcul du terme d'ajustement des findings et stone
    adjustment = (ifind + istone) - (rstone + rfind)
    adjusted_delivery_gwt = rgwt - adjustment + rscrap
    loss_g = igwt + iscrap - adjusted_delivery_gwt
    denom = rgwt - ((ifind + istone) - (rstone + rfind))
    if denom == 0:
        loss_pct = None
    else:
        loss_pct = (loss_g / denom) * 100.0
    return loss_g, loss_pct


def compute_working_duration(start: datetime, end: datetime) -> float:
    """Calcule la durée de travail en heures ouvrables entre deux dates.

    Les heures ouvrables sont définies du lundi au samedi (0-5) de 7h30 à 18h40. Les
    minutes en dehors de ces plages ne sont pas comptées. Retourne la durée totale en
    heures (float).
    """
    if start is None or end is None:
        return 0.0
    if end < start:
        return 0.0
    total_minutes = 0
    current = start
    # boucle jusqu'à la date de fin
    while current.date() <= end.date():
        weekday = current.weekday()  # 0=lundi, 6=dimanche
        # ignorer le dimanche
        if weekday != 6:
            day_start = datetime.combine(current.date(), time(7, 30))
            day_end = datetime.combine(current.date(), time(18, 40))
            # calcul de la période de travail pertinente pour ce jour
            period_start = max(current, day_start)
            period_end = min(end, day_end)
            if period_start < period_end:
                delta = period_end - period_start
                total_minutes += delta.total_seconds() / 60.0
        # passe au début du lendemain
        current = datetime.combine(current.date() + timedelta(days=1), time.min)
    return total_minutes / 60.0


def format_duration(hours: float) -> str:
    """Formate une durée exprimée en heures en ``XX h XX min``."""
    total_minutes = int(round(hours * 60))
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{h:02d} h {m:02d} min"


class DataManager:
    """Gestionnaire de base de données pour les comptes JBA et leurs étapes."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        ensure_db_exists(self.db_path)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass
        return conn

    def _renumber_step_positions(self, cursor: sqlite3.Cursor, job_id: int):
        """Réattribue des positions séquentielles pour les étapes d'un job donné."""

        cursor.execute(
            "SELECT id FROM steps WHERE job_id=? ORDER BY position ASC, id ASC",
            (job_id,),
        )
        step_ids = [row[0] for row in cursor.fetchall()]
        for index, step_id in enumerate(step_ids, start=1):
            cursor.execute(
                "UPDATE steps SET position=? WHERE id=?",
                (index, step_id),
            )

    # ------------------------------------------------------------------
    # Gestion des ouvriers
    def list_workers(self):
        """Retourne la liste des ouvriers enregistrés dans la base."""

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, name, ordre
                FROM workers
                ORDER BY CASE WHEN ordre IS NULL THEN 1 ELSE 0 END,
                         ordre,
                         name COLLATE NOCASE
                """
            )
            return cur.fetchall()
        finally:
            conn.close()

    def add_worker(self, name: str, ordre=None) -> int:
        """Ajoute un ouvrier après validation du nom."""

        if name is None:
            raise ValueError("Le nom de l'ouvrier est requis.")

        cleaned_name = " ".join(name.split())
        if not cleaned_name:
            raise ValueError("Le nom de l'ouvrier ne peut pas être vide.")

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM workers WHERE name = ? COLLATE NOCASE",
                (cleaned_name,),
            )
            if cur.fetchone():
                raise ValueError(
                    f"Un ouvrier nommé '{cleaned_name}' existe déjà."
                )

            cur.execute(
                "INSERT INTO workers (name, ordre) VALUES (?, ?)",
                (cleaned_name, ordre),
            )
            worker_id = cur.lastrowid
            conn.commit()
            return worker_id
        finally:
            conn.close()

    def rename_worker(self, worker_id: int, new_name: str) -> bool:
        """Renomme un ouvrier après vérification de l'unicité."""

        if new_name is None:
            raise ValueError("Le nom de l'ouvrier est requis.")

        cleaned_name = " ".join(new_name.split())
        if not cleaned_name:
            raise ValueError("Le nom de l'ouvrier ne peut pas être vide.")

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM workers WHERE name = ? COLLATE NOCASE AND id != ?",
                (cleaned_name, worker_id),
            )
            if cur.fetchone():
                raise ValueError(
                    f"Un ouvrier nommé '{cleaned_name}' existe déjà."
                )

            cur.execute(
                "UPDATE workers SET name = ? WHERE id = ?",
                (cleaned_name, worker_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def delete_worker(self, worker_id: int) -> bool:
        """Supprime un ouvrier identifié par ``worker_id``."""

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def list_jobs(self):
        """Retourne la liste des jobs avec quelques informations de synthèse."""
        conn = self._connect()
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT id, ref, description, status, start_time, end_time, total_loss, total_loss_pct
            FROM jobs
            ORDER BY id ASC
            """
        ).fetchall()
        conn.close()
        return rows

    def generate_new_ref(self) -> str:
        """Génère une nouvelle référence JBA en incrémentant le plus grand numéro existant."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT ref FROM jobs WHERE ref LIKE 'JBA %' ORDER BY ref DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            return "JBA 001"
        last_ref = row[0]
        try:
            last_num = int(last_ref.split()[1])
            next_num = last_num + 1
        except Exception:
            next_num = 1
        return f"JBA {next_num:03d}"

    def add_job(self, ref: str, description: str) -> int:
        """Ajoute un nouveau job avec la référence et la description fournies."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
          "INSERT INTO jobs (ref, description, status) VALUES (?, ?, ?)",
          (ref, description, None),
        )
        job_id = cur.lastrowid
        conn.commit()
        conn.close()

        return job_id

       
    
    def delete_job(self, job_id: int):
        """Supprime un job et toutes ses étapes et détails dans la base locale."""

        # --- 1. SUPPRESSION LOCALE ---
        conn = self._connect()
        cur = conn.cursor()

        # Supprimer les livraisons liées à ce job (via les étapes)
        cur.execute(
            "DELETE FROM deliveries WHERE step_id IN (SELECT id FROM steps WHERE job_id=?)",
            (job_id,),
        )

        # Supprimer les détails liés à ce job (via les étapes)
        cur.execute(
            "DELETE FROM details WHERE step_id IN (SELECT id FROM steps WHERE job_id=?)",
            (job_id,),
        )
        cur.execute(
            "DELETE FROM issues WHERE step_id IN (SELECT id FROM steps WHERE job_id=?)",
            (job_id,),
        )

        # Supprimer les étapes liées à ce job
        cur.execute("DELETE FROM steps WHERE job_id=?", (job_id,))

        # Supprimer le job lui-même
        cur.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()
        conn.close()

    

    def add_step(self, job_id: int, name: str) -> int:
        """Ajoute une étape à un job et renvoie son identifiant local."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(position), 0) FROM steps WHERE job_id=?",
            (job_id,),
        )
        max_position = cur.fetchone()[0] or 0
        new_position = max_position + 1
        cur.execute(
            "INSERT INTO steps (job_id, name, position) VALUES (?, ?, ?)",
            (job_id, name, new_position),
        )
        step_id = cur.lastrowid
        if new_position == 1:
            cur.execute("SELECT description FROM jobs WHERE id=?", (job_id,))
            row = cur.fetchone()
            description = (row[0].strip() if row and row[0] else None)
            if description:
                cur.execute(
                    "UPDATE steps SET issue_desc=? WHERE id=?",
                    (description, step_id),
                )
                cur.execute(
                    "INSERT INTO issues (step_id, description) VALUES (?, ?)",
                    (step_id, description),
                )
        conn.commit()
        conn.close()

        return step_id

    
    def delete_step(self, step_id: int) -> int:
        """Supprime une étape et ses détails. Retourne l'identifiant du job."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT job_id FROM steps WHERE id=?", (step_id,))
        row = cur.fetchone()
        job_id = row[0] if row else None

        # Supprime les livraisons locales liées à cette étape
        cur.execute("DELETE FROM deliveries WHERE step_id=?", (step_id,))

        # Supprime les détails liés à l'étape
        cur.execute("DELETE FROM details WHERE step_id=?", (step_id,))
        cur.execute("DELETE FROM issues WHERE step_id=?", (step_id,))
        # Supprime l'étape elle-même
        cur.execute("DELETE FROM steps WHERE id=?", (step_id,))
        if job_id is not None:
            self._renumber_step_positions(cur, job_id)
        conn.commit()
        conn.close()

        # Recalcul du job associé
        if job_id is not None:
            self.recalc_job_metrics(job_id)
        return job_id

    def update_step_order(self, job_id: int, ordered_step_ids: Sequence[int]):
        """Met à jour l'ordre des étapes pour un job donné en fonction de ``ordered_step_ids``."""

        if not ordered_step_ids:
            return

        conn = self._connect()
        try:
            cur = conn.cursor()
            for position, step_id in enumerate(ordered_step_ids, start=1):
                cur.execute(
                    "UPDATE steps SET position=? WHERE id=? AND job_id=?",
                    (position, step_id, job_id),
                )
            conn.commit()
        finally:
            conn.close()



    def _sync_detail_totals(self, step_id: int, type_: str):
        """Synchronise les totaux de l'étape à partir des détails."""

        field_mapping = {
            "findings": "issue_finding",
            "stone": "issue_stone",
            "return_findings": "return_finding",
            "return_stone": "return_stone",
        }
        field_name = field_mapping.get(type_)
        if not field_name:
            return
        total = self.get_detail_sum(step_id, type_)
        self.update_step(step_id, **{field_name: total})

    def add_detail(self, step_id: int, type_: str, description: str, pcs: int, gwt: float):
        """Ajoute un détail pour une étape (Findings ou Stone)."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO details (step_id, type, description, pcs, gwt) VALUES (?, ?, ?, ?, ?)",
            (step_id, type_, description, pcs, gwt),
        )
        detail_id = cur.lastrowid
        conn.commit()
        conn.close()
        self._sync_detail_totals(step_id, type_)
        return detail_id

    def update_detail_and_sync(self, detail_id: int, values: dict) -> bool:
        """Met à jour un détail existant et synchronise les totaux associés."""

        if not values:
            return False

        allowed_fields = {"description", "pcs", "gwt"}
        fields = []
        params = []
        for key in allowed_fields:
            if key in values:
                fields.append(f"{key} = ?")
                params.append(values[key])

        if not fields:
            return False

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT step_id, type FROM details WHERE id=?", (detail_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        step_id, type_ = row
        cur.execute(
            f"UPDATE details SET {', '.join(fields)} WHERE id=?",
            (*params, detail_id),
        )
        conn.commit()
        conn.close()
        self._sync_detail_totals(step_id, type_)
        return True


    def get_details(self, step_id: int, type_: str):
        """Retourne les détails pour un type (findings/stone) d'une étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, description, pcs, gwt FROM details WHERE step_id=? AND type=?",
            (step_id, type_),
        )
        rows = cur.fetchall()
        conn.close()
        return rows

    def get_detail_sum(self, step_id: int, type_: str) -> float:
        """Retourne la somme des GWT pour les détails d'un type."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT SUM(gwt) FROM details WHERE step_id=? AND type=?",
            (step_id, type_),
        )
        row = cur.fetchone()
        conn.close()
        return row[0] or 0.0


    def delete_detail(self, detail_id: int) -> bool:
        """Supprime un enregistrement de détail (stone ou finding)."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT step_id, type FROM details WHERE id=?", (detail_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        step_id, type_ = row
        cur.execute("DELETE FROM details WHERE id=?", (detail_id,))
        conn.commit()
        conn.close()
        self._sync_detail_totals(step_id, type_)
        return True


    def get_issues(self, step_id: int):
        """Retourne toutes les issues d'une étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, issue_time, gwt, scrap, stone, finding, description,
                   gwt_formula, scrap_formula, stone_formula, finding_formula
            FROM issues
            WHERE step_id=?
            ORDER BY id
            """,
            (step_id,),
        )
        rows = cur.fetchall()
        conn.close()
        return rows

    def add_issue(
        self,
        step_id: int,
        issue_time,
        gwt,
        scrap,
        stone,
        finding,
        description=None,
        gwt_formula=None,
        scrap_formula=None,
        stone_formula=None,
        finding_formula=None,
    ):
        """Ajoute une issue pour une étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO issues (
                step_id, issue_time, gwt, scrap, stone, finding, description,
                gwt_formula, scrap_formula, stone_formula, finding_formula
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                step_id,
                issue_time,
                gwt,
                scrap,
                stone,
                finding,
                description,
                gwt_formula,
                scrap_formula,
                stone_formula,
                finding_formula,
            ),
        )
        issue_id = cur.lastrowid
        conn.commit()
        conn.close()


    def delete_issue(self, issue_id: int):
        """Supprime une issue."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM issues WHERE id=?", (issue_id,))
        conn.commit()
        conn.close()


    def get_deliveries(self, step_id: int):
        """Retourne toutes les livraisons d'une étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, delivery_time, gwt, scrap, return_stone, return_finding,
                   description, gwt_formula, scrap_formula,
                   return_stone_formula, return_finding_formula,
                   scrap_delivered
            FROM deliveries
            WHERE step_id=?
            ORDER BY id
            """,
            (step_id,),
        )
        rows = cur.fetchall()
        conn.close()
        return rows

    def add_delivery(
        self,
        step_id: int,
        delivery_time,
        gwt,
        scrap,
        return_stone,
        return_finding,
        description=None,
        gwt_formula=None,
        scrap_formula=None,
        return_stone_formula=None,
        return_finding_formula=None,
        *,
        scrap_delivered: int = 0,
    ):
        """Ajoute une livraison pour une étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO deliveries (
                step_id, delivery_time, gwt, scrap, return_stone, return_finding,
                description, gwt_formula, scrap_formula,
                return_stone_formula, return_finding_formula,
                scrap_delivered
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                step_id,
                delivery_time,
                gwt,
                scrap,
                return_stone,
                return_finding,
                description,
                gwt_formula,
                scrap_formula,
                return_stone_formula,
                return_finding_formula,
                scrap_delivered,
            ),
        )
        delivery_id = cur.lastrowid
        conn.commit()
        conn.close()

    def delete_delivery(self, delivery_id: int):
        """Supprime une livraison."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM deliveries WHERE id=?", (delivery_id,))
        conn.commit()
        conn.close()

    def get_current_step(self, job_id: int):
        """Retourne la première étape non encore livrée pour un job.

        Si toutes les étapes ont un return_time, renvoie None.
        Retourne un tuple (step_id, name, worker, issue_time).
        """
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, worker, issue_time
            FROM steps
            WHERE job_id=? AND (return_time IS NULL OR return_time='')
            ORDER BY position ASC, id ASC
            LIMIT 1
            """,
            (job_id,),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return row  # (id, name, worker, issue_time)
        return None

    def get_last_completed_step(self, job_id: int):
        """Retourne la dernière étape terminée pour un job donné.

        La requête sélectionne la dernière étape avec un ``return_time`` non vide,
        triée par date décroissante. Renvoie un tuple
        ``(step_id, name, worker, return_time)`` ou ``None`` si aucune étape
        correspondante n'existe.
        """

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, worker, return_time
            FROM steps
            WHERE job_id=?
              AND return_time IS NOT NULL
              AND return_time != ''
            ORDER BY return_time DESC, id DESC
            LIMIT 1
            """,
            (job_id,),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return row  # (id, name, worker, return_time)
        return None

    def get_job(self, job_id: int):
        """Retourne les informations du job et ses étapes triées dans l'ordre défini."""
        conn = self._connect()
        cur = conn.cursor()
        job = cur.execute(
            "SELECT id, ref, description, status, start_time, end_time, total_loss, total_loss_pct FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        steps = cur.execute(
            """
            SELECT
                id,
                job_id,
                name,
                worker,
                issue_time,
                issue_desc,
                issue_gwt,
                issue_scrap,
                issue_stone,
                issue_finding,
                return_time,
                return_desc,
                return_gwt,
                return_scrap,
                return_stone,
                return_finding,
                loss_weight,
                loss_pct,
                position
            FROM steps
            WHERE job_id=?
            ORDER BY position ASC, id ASC
            """,
            (job_id,),
        ).fetchall()
        conn.close()
        return job, steps

    def update_step(self, step_id: int, **kwargs):
        """Met à jour un enregistrement d'étape avec les champs spécifiés.

        Exemple d'appel : update_step(step_id, worker='Nom', issue_time='2025-07-05 12:00:00')
        """
        if not kwargs:
            return
        fields = []
        values = []
        for key, value in kwargs.items():
            fields.append(f"{key} = ?")
            values.append(value)
        values.append(step_id)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            f"UPDATE steps SET {', '.join(fields)} WHERE id = ?", values
        )
        conn.commit()
        conn.close()

    def recalc_job_metrics(self, job_id: int):
        """Recalcule les dates de début/fin et les pertes totales d'un job."""
        conn = self._connect()
        cur = conn.cursor()
        # Récupère start_time et end_time
        cur.execute(
            "SELECT issue_time FROM steps WHERE job_id=? AND issue_time IS NOT NULL ORDER BY issue_time ASC LIMIT 1",
            (job_id,),
        )
        row = cur.fetchone()
        start_time = row[0] if row else None
        cur.execute(
            "SELECT return_time FROM steps WHERE job_id=? AND return_time IS NOT NULL ORDER BY return_time DESC LIMIT 1",
            (job_id,),
        )
        row = cur.fetchone()
        end_time = row[0] if row else None
        # Calcule les pertes totales
        cur.execute(
            "SELECT SUM(loss_weight), SUM(loss_weight * 100.0 / return_gwt) FROM steps WHERE job_id=? AND loss_weight IS NOT NULL AND return_gwt IS NOT NULL",
            (job_id,),
        )
        total_loss, _old_sum_pct = cur.fetchone()
        # Calcule asa_vita = GWT DELIVERY de la dernière étape livrée (avec retour)
        cur.execute(
            "SELECT return_gwt, issue_finding, issue_stone, return_stone, return_finding FROM steps WHERE job_id=? AND return_time IS NOT NULL AND return_time != '' ORDER BY return_time DESC LIMIT 1",
            (job_id,),
        )
        row_end = cur.fetchone()
        asa_vita = None
        if row_end:
            rgwt, ifind, istone, rstone, rfind = row_end
            # terme d'ajustement : Findings ISSUE + Stone ISSUE - Return Stone - Return Findings
            ifind = float(ifind) if ifind not in (None, '') else 0.0
            istone = float(istone) if istone not in (None, '') else 0.0
            rstone = float(rstone) if rstone not in (None, '') else 0.0
            rfind = float(rfind) if rfind not in (None, '') else 0.0
            asa_vita = rgwt - ((ifind + istone) - (rstone + rfind))
        # Calcul du pourcentage global
        total_loss_pct = None
        if asa_vita and asa_vita != 0 and total_loss is not None:
            total_loss_pct = (total_loss / asa_vita) * 100.0
        # Met à jour
        cur.execute(
            "UPDATE jobs SET start_time=?, end_time=?, total_loss=?, total_loss_pct=? WHERE id=?",
            (start_time, end_time, total_loss, total_loss_pct, job_id),
        )


        # Détermine le statut : nom de l'étape en cours le cas échéant
        cur.execute(
            """
            SELECT id, name
            FROM steps
            WHERE job_id=?
              AND (return_time IS NULL OR return_time='')
            ORDER BY position ASC, id ASC
            LIMIT 1
            """,
            (job_id,),
        )
        current = cur.fetchone()
        cur.execute("SELECT status FROM jobs WHERE id=?", (job_id,))
        row_status = cur.fetchone()
        existing_status = row_status[0] if row_status else None

        if current:
            status = current[1]
        else:
            existing_normalized = (
                existing_status.strip().lower()
                if isinstance(existing_status, str)
                else ""
            )
            if existing_normalized in {"fini", "terminé"}:
                status = existing_status
            else:
                status = None

        cur.execute("UPDATE jobs SET status=? WHERE id=?", (status, job_id))
        conn.commit()
        conn.close()

    def update_loss_for_step(self, step_id: int):
        """Recalcule et met à jour la perte pour une étape spécifique."""
        conn = self._connect()
        cur = conn.cursor()
        # Récupère les données d'issue de l'étape
        cur.execute(
            "SELECT issue_gwt, issue_scrap, issue_finding, issue_stone FROM steps WHERE id=?",
            (step_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return
        igwt, iscrap, ifind, istone = row

        # Récupère la somme des livraisons et la dernière date
        cur.execute(
            "SELECT SUM(gwt), SUM(scrap), SUM(return_stone), SUM(return_finding), MAX(delivery_time) FROM deliveries WHERE step_id=?",
            (step_id,),
        )
        drow = cur.fetchone()
        if drow:
            total_gwt, total_scrap, total_rstone, total_rfind, last_time = drow
        else:
            total_gwt = total_scrap = total_rstone = total_rfind = last_time = None

        total_gwt = total_gwt or 0.0
        total_scrap = total_scrap or 0.0
        total_rstone = total_rstone or 0.0
        total_rfind = total_rfind or 0.0

        loss_wt, loss_pct = compute_loss(
            igwt,
            iscrap,
            total_gwt,
            total_scrap,
            total_rstone,
            total_rfind,
            issue_finding=ifind,
            issue_stone=istone,
        )
        cur.execute(
            """
            UPDATE steps
            SET return_time=?, return_gwt=?, return_scrap=?, return_stone=?, return_finding=?,
                loss_weight=?, loss_pct=?
            WHERE id=?
            """,
            (last_time, total_gwt, total_scrap, total_rstone, total_rfind, loss_wt, loss_pct, step_id),
        )
        conn.commit()
        conn.close()

    def export_jobs_to_excel(self, filepath: str):
        """Exporte tous les jobs terminés (statut Terminé) dans un fichier Excel."""
        if pd is None:
            raise RuntimeError("La bibliothèque pandas n'est pas installée, export impossible.")
        conn = self._connect()
        # Récupère toutes les informations
        jobs_df = pd.read_sql_query(
            "SELECT * FROM jobs WHERE status='Terminé'", conn
        )
        steps_df = pd.read_sql_query(
            "SELECT * FROM steps WHERE job_id IN (SELECT id FROM jobs WHERE status='Terminé')",
            conn,
        )
        # Écrit dans un fichier Excel avec deux feuilles
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            jobs_df.to_excel(writer, sheet_name='jobs', index=False)
            steps_df.to_excel(writer, sheet_name='steps', index=False)
        conn.close()

    def get_monthly_summary(self):
        """
        Calcule un bilan mensuel.

        Pour chaque job terminé, récupère la perte totale du job, identifie la
        dernière étape livrée et calcule le travail final livré. Les totaux
        sont ensuite agrégés par mois.

        Retourne une liste de tuples (année-mois, total_production, total_loss, loss_pct).
        """
        conn = self._connect()
        cur = conn.cursor()
        # Sélectionne les jobs terminés
        cur.execute(
            "SELECT id, end_time, total_loss FROM jobs WHERE status='Fini' OR status='Terminé'"
        )
        jobs = cur.fetchall()
        summary = {}
        for job in jobs:
            job_id, end_date, total_loss = job
            total_loss = total_loss or 0.0
            # Somme des GWT livrés pour toutes les étapes « Polissage Final »
            cur.execute(
                """
                SELECT COALESCE(SUM(deliveries.gwt), 0)
                FROM deliveries
                JOIN steps ON deliveries.step_id = steps.id
                WHERE steps.job_id = ? AND LOWER(steps.name) = 'polissage final'
                """,
                (job_id,),
            )
            row_last = cur.fetchone()
            final_work = row_last[0] if row_last and row_last[0] is not None else 0.0
            # Clé mensuelle basée sur la date de fin
            if end_date:
                try:
                    dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                    month_key = dt.strftime("%Y-%m")
                except Exception:
                    month_key = "Inconnu"
            else:
                month_key = "Inconnu"
            if month_key not in summary:
                summary[month_key] = {
                    "total_production": 0.0,
                    "total_loss": 0.0,
                }
            summary[month_key]["total_production"] += final_work or 0.0
            summary[month_key]["total_loss"] += total_loss
        result = []
        for month_key in sorted(summary.keys()):
            data = summary[month_key]
            prod = data["total_production"]
            loss = data["total_loss"]
            loss_pct = (loss / prod * 100.0) if prod else 0.0
            result.append((month_key, prod, loss, loss_pct))
        conn.close()
        return result

    def get_worker_monthly_summary(self):
        """Retourne un bilan mensuel détaillé par ouvrier et par étape."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT worker, name, issue_time, return_time, return_gwt, loss_weight
            FROM steps
            WHERE worker IS NOT NULL AND worker != ''
              AND return_time IS NOT NULL AND return_time != ''
            """
        )
        rows = cur.fetchall()
        conn.close()

        summary = {}
        for worker, step_name, issue_t, return_t, rgwt, loss_w in rows:
            try:
                dt = datetime.strptime(return_t, "%Y-%m-%d %H:%M:%S")
                month_key = dt.strftime("%m/%Y")
            except Exception:
                continue
            if month_key not in summary:
                summary[month_key] = {}
            if worker not in summary[month_key]:
                summary[month_key][worker] = {}
            data = summary[month_key][worker].setdefault(
                step_name,
                {"gwt": 0.0, "duration": 0.0, "loss_g": 0.0},
            )
            data["gwt"] += rgwt or 0.0
            data["loss_g"] += loss_w or 0.0
            try:
                dt_issue = datetime.strptime(issue_t, "%Y-%m-%d %H:%M:%S")
                duration = compute_working_duration(dt_issue, dt)
            except Exception:
                duration = 0.0
            data["duration"] += duration

        # Calcule les pourcentages
        for workers in summary.values():
            for steps in workers.values():
                for vals in steps.values():
                    gwt = vals["gwt"]
                    loss_g = vals["loss_g"]
                    vals["loss_pct"] = (loss_g / gwt * 100.0) if gwt else 0.0
        return summary

    def get_job_summary(self, job_id: int):
        """Renvoie un résumé pour un job.

        Le résumé inclut la référence, le premier ouvrier, les dates de début et de fin,
        la durée (différence simple), la perte totale, le pourcentage de perte et
        le travail fini (ASA Vita) calculé comme la somme des GWT livrés pour toutes
        les étapes « Polissage Final ».

        Le pourcentage de perte total est recalculé comme Perte totale ÷ Travail fini
        lorsque ce dernier est strictement positif.
        """
        conn = self._connect()
        cur = conn.cursor()
        # Récupération des informations du job
        cur.execute(
            "SELECT ref, start_time, end_time, total_loss, total_loss_pct FROM jobs WHERE id=?",
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return None
        ref, start_time, end_time, total_loss, saved_loss_pct = row
        # Premier ouvrier : premier worker défini dans les étapes
        cur.execute(
            "SELECT worker FROM steps WHERE job_id=? AND worker IS NOT NULL AND worker != '' ORDER BY position ASC, id ASC LIMIT 1",
            (job_id,),
        )
        first_worker_row = cur.fetchone()
        first_worker = first_worker_row[0] if first_worker_row else ''
        # Conversion des dates en datetime
        dt_start = None
        dt_end = None
        try:
            if start_time:
                dt_start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt_start = None
        try:
            if end_time:
                dt_end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt_end = None
        # Durée simple en heures
        duration_hours = 0.0
        if dt_start and dt_end:
            try:
                delta = dt_end - dt_start
                duration_hours = delta.total_seconds() / 3600.0
            except Exception:
                duration_hours = 0.0
        # Calcul du travail fini (ASA Vita)
        # Somme des GWT livrés pour les étapes « Polissage Final »
        cur.execute(
            """
            SELECT COALESCE(SUM(deliveries.gwt), 0)
            FROM deliveries
            JOIN steps ON deliveries.step_id = steps.id
            WHERE steps.job_id = ? AND LOWER(steps.name) = 'polissage final'
            """,
            (job_id,),
        )
        row_final = cur.fetchone()
        final_work = row_final[0] if row_final and row_final[0] is not None else 0.0
        # Calcul des morceaux confirmés comme livrés
        cur.execute(
            """
            SELECT SUM(scrap)
            FROM deliveries
            WHERE step_id IN (SELECT id FROM steps WHERE job_id=?)
              AND COALESCE(scrap_delivered, 0) = 1
            """,
            (job_id,),
        )
        row_delivery_total = cur.fetchone()
        piece_total = row_delivery_total[0] if row_delivery_total and row_delivery_total[0] else 0.0
        # Perte totale
        total_loss_val = total_loss or 0.0
        # Pourcentage de perte recalculé
        loss_pct = None
        if final_work and final_work != 0:
            try:
                loss_pct = (total_loss_val / final_work) * 100.0
            except Exception:
                loss_pct = saved_loss_pct
        else:
            loss_pct = saved_loss_pct
        conn.close()
        return {
            'ref': ref,
            'worker': first_worker,
            'start_time': dt_start,
            'end_time': dt_end,
            'duration_hours': duration_hours,
            'total_loss': total_loss_val,
            'total_loss_pct': loss_pct,
            'final_work': final_work,
            'piece_total': piece_total,
        }
    def update_job_fields(self, job_id: int, **fields):
        """Met à jour les champs du job identifié par ``job_id``."""

        if not fields:
            return False

        # Mise à jour locale
        keys = list(fields.keys())
        values = list(fields.values())
        set_clause = ", ".join(f"{key} = ?" for key in keys)
        query = f"UPDATE jobs SET {set_clause} WHERE id = ?"

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(query, values + [job_id])
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


    def reset_database(self, reset_name: str) -> int:
        """Archive uniquement les travaux terminés et les supprime de la base active."""

        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("SELECT * FROM jobs ORDER BY id")
        all_jobs = cur.fetchall()
        finished_statuses = {"fini", "terminé", "termine"}
        finished_jobs = [
            dict(row)
            for row in all_jobs
            if (row["status"] or "").strip().lower() in finished_statuses
        ]

        if not finished_jobs:
            conn.close()
            return 0

        job_ids = [job["id"] for job in finished_jobs]
        placeholders_jobs = ",".join("?" for _ in job_ids)

        cur.execute("SELECT * FROM workers ORDER BY id")
        workers = [dict(row) for row in cur.fetchall()]

        cur.execute(
            f"SELECT * FROM steps WHERE job_id IN ({placeholders_jobs}) ORDER BY id",
            job_ids,
        )
        steps = [dict(row) for row in cur.fetchall()]
        step_ids = [step["id"] for step in steps]

        details = []
        issues = []
        deliveries = []
        if step_ids:
            placeholders_steps = ",".join("?" for _ in step_ids)
            cur.execute(
                f"SELECT * FROM details WHERE step_id IN ({placeholders_steps}) ORDER BY id",
                step_ids,
            )
            details = [dict(row) for row in cur.fetchall()]
            cur.execute(
                f"SELECT * FROM issues WHERE step_id IN ({placeholders_steps}) ORDER BY id",
                step_ids,
            )
            issues = [dict(row) for row in cur.fetchall()]
            cur.execute(
                f"SELECT * FROM deliveries WHERE step_id IN ({placeholders_steps}) ORDER BY id",
                step_ids,
            )
            deliveries = [dict(row) for row in cur.fetchall()]

        archives_dir = os.path.join(BASE_DIR, "archives")
        os.makedirs(archives_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        display_name = reset_name.strip() if reset_name else reset_name
        sanitized_source = display_name or "reset"
        safe_name = re.sub(
            r"[^\w.-]+",
            "_",
            sanitized_source.replace("/", "_").replace("\\", "_"),
        ) or "reset"
        archive_filename = f"{timestamp}_{safe_name}.db"
        archive_path = os.path.join(archives_dir, archive_filename)

        ensure_db_exists(archive_path)

        def _insert_rows(cursor, table, rows):
            if not rows:
                return
            columns = list(rows[0].keys())
            placeholders = ",".join("?" for _ in columns)
            column_list = ", ".join(columns)
            values = [tuple(row[col] for col in columns) for row in rows]
            cursor.executemany(
                f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
                values,
            )

        archive_conn = sqlite3.connect(archive_path)
        try:
            archive_cursor = archive_conn.cursor()
            _insert_rows(archive_cursor, "workers", workers)
            _insert_rows(archive_cursor, "jobs", finished_jobs)
            _insert_rows(archive_cursor, "steps", steps)
            _insert_rows(archive_cursor, "details", details)
            _insert_rows(archive_cursor, "issues", issues)
            _insert_rows(archive_cursor, "deliveries", deliveries)
            archive_conn.commit()
        except Exception:
            archive_conn.rollback()
            archive_conn.close()
            try:
                os.remove(archive_path)
            except OSError:
                pass
            raise
        else:
            archive_conn.close()

        try:
            conn.execute("BEGIN")
            if step_ids:
                placeholders_steps = ",".join("?" for _ in step_ids)
                cur.execute(
                    f"DELETE FROM deliveries WHERE step_id IN ({placeholders_steps})",
                    step_ids,
                )
                cur.execute(
                    f"DELETE FROM details WHERE step_id IN ({placeholders_steps})",
                    step_ids,
                )
                cur.execute(
                    f"DELETE FROM issues WHERE step_id IN ({placeholders_steps})",
                    step_ids,
                )
                cur.execute(
                    f"DELETE FROM steps WHERE id IN ({placeholders_steps})",
                    step_ids,
                )
            cur.execute(
                f"DELETE FROM jobs WHERE id IN ({placeholders_jobs})",
                job_ids,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        resets_file = os.path.join(archives_dir, RESET_HISTORY_FILENAME)
        resets = []
        if os.path.exists(resets_file):
            try:
                with open(resets_file, "r", encoding="utf-8") as f:
                    resets = json.load(f)
            except Exception:
                resets = []

        resets.append(
            {
                "name": display_name or reset_name,
                "timestamp": timestamp,
                "path": archive_path,
                "archived_jobs": len(job_ids),
                "app": APP_CODE,
                "source_db": os.path.basename(self.db_path),
            }
        )
        with open(resets_file, "w", encoding="utf-8") as f:
            json.dump(resets, f, ensure_ascii=False, indent=2)

        return len(job_ids)

    def list_resets(self):
        """Retourne la liste des réinitialisations archivées, triées par date."""
        archives_dir = os.path.join(BASE_DIR, "archives")
        resets_file = os.path.join(archives_dir, RESET_HISTORY_FILENAME)
        if os.path.exists(resets_file):
            try:
                with open(resets_file, "r", encoding="utf-8") as f:
                    resets = json.load(f)
            except Exception:
                return []
        else:
            legacy_file = os.path.join(archives_dir, "resets.json")
            if not os.path.exists(legacy_file):
                return []
            try:
                with open(legacy_file, "r", encoding="utf-8") as f:
                    legacy_resets = json.load(f)
            except Exception:
                return []
            allowed_db = os.path.basename(self.db_path)
            resets = [
                r
                for r in legacy_resets
                if r.get("app") == APP_CODE or r.get("source_db") == allowed_db
            ]
            if resets:
                os.makedirs(archives_dir, exist_ok=True)
                try:
                    with open(resets_file, "w", encoding="utf-8") as f:
                        json.dump(resets, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
        return sorted(resets, key=lambda r: r.get("timestamp", ""), reverse=True)


def build_monthly_totals(monthly_summary):
    """Convertit la liste de synthèse mensuelle en dictionnaire par mois."""
    totals = {}
    for entry in monthly_summary or []:
        if not entry:
            continue
        month_key, final_work, total_loss, *rest = entry
        display_key = month_key
        try:
            display_key = datetime.strptime(month_key, "%Y-%m").strftime("%m/%Y")
        except Exception:
            pass
        month_totals = totals.setdefault(display_key, {"final_work": 0.0, "total_loss": 0.0})
        month_totals["final_work"] += final_work or 0.0
        month_totals["total_loss"] += total_loss or 0.0
    return totals


def display_monthly_summary(master, data, monthly_totals, title):
    """Affiche un bilan mensuel à partir de données fournies."""
    if not data:
        messagebox.showinfo(title, "Aucune donnée disponible pour le moment.")
        return
    win = tk.Toplevel(master)
    win.title(title)
    try:
        bg_color = master.cget('bg')
        win.configure(bg=bg_color)
    except Exception:
        pass
    cols = ("Étape", "GWT", "Durée", "Perte (g)", "Perte (%)")
    tree = ttk.Treeview(win, columns=cols, show="tree headings")
    tree.heading("#0", text="Section")
    for col in cols:
        tree.heading(col, text=col)
        tree.column(col, anchor="center", minwidth=80, width=100)

    for name_norm, color in STEP_COLOR_MAP.items():
        tree.tag_configure(name_norm, background=color)

    monthly_totals = monthly_totals or {}
    for month in sorted(data.keys()):
        month_id = tree.insert('', 'end', text=month, open=False)
        for worker in sorted(data[month].keys()):
            worker_id = tree.insert(month_id, 'end', text=worker, open=False)
            for step_name, vals in data[month][worker].items():
                name_norm = step_name.lower()
                tree.insert(
                    worker_id,
                    'end',
                    text='',
                    values=(
                        step_name,
                        f"{vals['gwt']:.2f}",
                        format_duration(vals['duration']),
                        f"{vals['loss_g']:.2f}",
                        f"{vals['loss_pct']:.2f}%",
                    ),
                    tags=(name_norm,),
                )
        month_totals = monthly_totals.get(month, {})
        month_gwt_total = month_totals.get("final_work", 0.0)
        month_loss_total = month_totals.get("total_loss", 0.0)
        total_pct = (month_loss_total / month_gwt_total * 100.0) if month_gwt_total else 0.0
        tree.insert(
            month_id,
            'end',
            text='TOTAL',
            values=(
                '',
                f"{month_gwt_total:.2f}",
                '',
                f"{month_loss_total:.2f}",
                f"{total_pct:.2f}%",
            ),
            tags=('total_row',),
        )

    tree.tag_configure('total_row', font=(DEFAULT_FONT_FAMILY, 9, 'bold'))
    tree.pack(fill='both', expand=True)


class StepFrame(ttk.LabelFrame):
    """Composant graphique représentant une étape de travail."""

    def __init__(
        self,
        parent,
        step_data,
        data_manager: DataManager,
        on_update_callback,
        reload_callback,
        worker_fetcher=None,
    ):
        """
        :param parent: conteneur Tkinter
        :param step_data: tuple représentant l'enregistrement de la table steps
        :param data_manager: instance de DataManager
        :param on_update_callback: fonction appelée après mise à jour pour recalculer le job
        :param reload_callback: fonction pour recharger toutes les étapes après suppression
        """
        # step_data structure correspond à SELECT * FROM steps: id, job_id, name, worker, issue_time,
        # issue_desc, issue_gwt, issue_scrap, issue_stone, issue_finding, return_time,
        # return_desc, return_gwt, return_scrap, return_stone, return_finding, loss_weight, loss_pct, position
        step_id = step_data[0]
        self.step_id = step_id
        self.job_id = step_data[1]
        self.name = step_data[2]
        self.position = step_data[18] if len(step_data) > 18 else None
        self.data_manager = data_manager
        self.on_update_callback = on_update_callback
        self.reload_callback = reload_callback
        self.worker_fetcher = worker_fetcher
        super().__init__(parent)
        header_frame = ttk.Frame(self)
        self.drag_handle = ttk.Label(
            header_frame,
            text="☰",
            width=2,
            anchor="center",
        )
        self.drag_handle.configure(cursor="fleur")
        self.drag_handle.pack(side="left", padx=(0, 4))
        self.title_label = ttk.Label(header_frame, text=self.name)
        self.title_label.configure(cursor="fleur")
        self.title_label.pack(side="left", fill="x", expand=True)
        self.configure(labelwidget=header_frame)
        self._header_frame = header_frame
        self._auto_save_after_id = None
        self._auto_save_in_progress = True
        self._auto_save_delay_ms = 200
        self._tracked_traces = []
        self._detail_windows = []
        self.bind("<Destroy>", self._on_destroy, add="+")
        self.columnconfigure(1, weight=1)
        # Réorganisation : deux sections (Issue / Delivery) et un panneau de résultats
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        # SECTION ISSUE (gauche)
        issue_frame = ttk.LabelFrame(self, text="ISSUE (Donné au départ)")
        issue_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        issue_frame.columnconfigure(1, weight=1)
        # Worker
        ttk.Label(issue_frame, text="Worker :").grid(row=0, column=0, sticky="e")
        self.worker_var = tk.StringVar(value=step_data[3] or '')
        self._track_stringvar(self.worker_var)
        self._worker_names_cache = []
        self.worker_entry = ttk.Combobox(
            issue_frame,
            textvariable=self.worker_var,
            state='readonly',
        )
        self.worker_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=2)
        self.worker_entry.configure(postcommand=self._refresh_worker_values)
        self.worker_entry.bind(
            "<<ComboboxSelected>>",
            lambda e: self._schedule_auto_save(),
            add="+",
        )
        self.update_worker_list()

        issue_container = ttk.Frame(issue_frame)
        issue_container.grid(row=1, column=0, columnspan=3, sticky="nsew")
        issue_container.columnconfigure(0, weight=1)

        self.issue_container = issue_container
        self.issue_lines = []

        issues = [
            {
                'issue_time': i[1],
                'gwt': i[2],           # pas de normalisation ici
                'scrap': i[3],
                'stone': grams_to_carats(i[4]),
                'finding': i[5],
                'description': i[6],
                'gwt_formula': i[7],
                'scrap_formula': i[8],
                'stone_formula': i[9],
                'finding_formula': i[10],
            }
            for i in self.data_manager.get_issues(step_id)
        ]


        def _has_meaningful_numbers(rows, keys):
            for row in rows:
                for key in keys:
                    value = row.get(key)
                    if value not in (None, ''):
                        return True
            return False

        fallback_issue = {
            'issue_time': step_data[4],
            'gwt': normalize_numeric_value(step_data[6]),
            'scrap': normalize_numeric_value(step_data[7]),
            'stone': grams_to_carats(step_data[8]),
            'finding': normalize_numeric_value(step_data[9]),
            'description': step_data[5],
            'gwt_formula': None,
            'scrap_formula': None,
            'stone_formula': None,
            'finding_formula': None,
        }

        def _has_fallback_data(data):
            return any(
                value not in (None, '')
                for value in (
                    data['issue_time'],
                    data['description'],
                    data['gwt'],
                    data['scrap'],
                    data['stone'],
                    data['finding'],
                )
            )

        if not issues and _has_fallback_data(fallback_issue):
            issues.append(fallback_issue)
        elif issues and not _has_meaningful_numbers(issues, ('gwt', 'scrap', 'stone', 'finding')) and _has_fallback_data(fallback_issue):
            primary_issue = issues[0]
            if not primary_issue.get('issue_time') and fallback_issue['issue_time']:
                primary_issue['issue_time'] = fallback_issue['issue_time']
            if not primary_issue.get('description') and fallback_issue['description']:
                primary_issue['description'] = fallback_issue['description']
            # Fallback ISSUE
            for key in ('gwt', 'scrap', 'stone', 'finding'):
                if primary_issue.get(key) in (None, '') and fallback_issue[key] not in (None, '', 0, 0.0):
                    primary_issue[key] = fallback_issue[key]


        for iss in issues:
            self._add_issue_line(iss)

        self.add_issue_btn = ttk.Button(
            self.issue_container, text="+", width=3, command=self._add_issue_line
        )
        self._update_issue_add_button()
        # SECTION DELIVERY (droite)
        delivery_frame = ttk.LabelFrame(self, text="DELIVERY (retour)")
        delivery_frame.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        delivery_frame.columnconfigure(0, weight=1)

        delivery_container = ttk.Frame(delivery_frame)
        delivery_container.grid(row=0, column=0, sticky="nsew")
        delivery_container.columnconfigure(0, weight=1)

        self.delivery_container = delivery_container
        self.delivery_lines = []

        deliveries = [
            {
                'delivery_time': d[1],
                'gwt': normalize_numeric_value(d[2]),
                'scrap': normalize_numeric_value(d[3]),
                'return_stone': grams_to_carats(d[4]),
                'return_finding': normalize_numeric_value(d[5]),
                'description': d[6],
                'gwt_formula': d[7],
                'scrap_formula': d[8],
                'return_stone_formula': d[9],
                'return_finding_formula': d[10],
                'scrap_delivered': bool(d[11]) if len(d) > 11 else False,
            }
            for d in self.data_manager.get_deliveries(step_id)
        ]

        fallback_delivery = {
            'delivery_time': step_data[10],
            'description': step_data[11],
            'gwt': normalize_numeric_value(step_data[12]),
            'scrap': normalize_numeric_value(step_data[13]),
            'return_stone': grams_to_carats(step_data[14]),
            'return_finding': normalize_numeric_value(step_data[15]),
            'gwt_formula': None,
            'scrap_formula': None,
            'return_stone_formula': None,
            'return_finding_formula': None,
        }

        def _has_delivery_fallback(data):
            return any(
                value not in (None, '')
                for value in (
                    data['delivery_time'],
                    data['description'],
                    data['gwt'],
                    data['scrap'],
                    data['return_stone'],
                    data['return_finding'],
                )
            )

        if not deliveries and _has_delivery_fallback(fallback_delivery):
            deliveries.append(fallback_delivery)
        elif deliveries and not _has_meaningful_numbers(deliveries, ('gwt', 'scrap', 'return_stone', 'return_finding')) and _has_delivery_fallback(fallback_delivery):
            primary_delivery = deliveries[0]
            if not primary_delivery.get('delivery_time') and fallback_delivery['delivery_time']:
                primary_delivery['delivery_time'] = fallback_delivery['delivery_time']
            if not primary_delivery.get('description') and fallback_delivery['description']:
                primary_delivery['description'] = fallback_delivery['description']
            for key in ('gwt', 'scrap', 'return_stone', 'return_finding'):
                if primary_delivery.get(key) in (None, '') and fallback_delivery[key] not in (None, '', 0, 0.0):
                    primary_delivery[key] = fallback_delivery[key]
            for formula_key in (
                'gwt_formula',
                'scrap_formula',
                'return_stone_formula',
                'return_finding_formula',
            ):
                if not primary_delivery.get(formula_key):
                    primary_delivery[formula_key] = None

        issue_gwt_tot = issue_scrap_tot = issue_stone_tot = issue_find_tot = 0.0
        first_issue_time = None
        for iss in issues:
            gwt_val = normalize_numeric_value(iss.get('gwt'))
            scrap_val = normalize_numeric_value(iss.get('scrap'))
            stone_val = normalize_numeric_value(iss.get('stone'))
            find_val = normalize_numeric_value(iss.get('finding'))
            issue_gwt_tot += gwt_val or 0.0
            issue_scrap_tot += scrap_val or 0.0
            stone_grams = carats_to_grams(stone_val)
            issue_stone_tot += stone_grams or 0.0
            issue_find_tot += find_val or 0.0
            dt = iss.get('issue_time')
            if dt and (first_issue_time is None or dt < first_issue_time):
                first_issue_time = dt

        total_gwt = total_scrap = total_stone = total_find = 0.0
        last_time = None
        for d in deliveries:
            self._add_delivery_line(d)
            gwt_val = normalize_numeric_value(d.get('gwt'))
            scrap_val = normalize_numeric_value(d.get('scrap'))
            stone_val = normalize_numeric_value(d.get('return_stone'))
            find_val = normalize_numeric_value(d.get('return_finding'))
            total_gwt += gwt_val or 0.0
            total_scrap += scrap_val or 0.0
            stone_grams = carats_to_grams(stone_val)
            total_stone += stone_grams or 0.0
            total_find += find_val or 0.0
            dt = d.get('delivery_time')
            if dt and (last_time is None or dt > last_time):
                last_time = dt

        self.add_delivery_btn = ttk.Button(
            self.delivery_container, text="+", width=3, command=self._add_delivery_line
        )
        self._update_add_button_position()
        # Bind double click to view details for return? not needed
        # RÉSULTATS (sous les deux cadres)
        loss_w = loss_pct = None
        if deliveries:
            loss_w, loss_pct = compute_loss(
                issue_gwt_tot,
                issue_scrap_tot,
                total_gwt,
                total_scrap,
                total_stone,
                total_find,
                issue_finding=issue_find_tot,
                issue_stone=issue_stone_tot,
            )

        result_frame = ttk.Frame(self)
        result_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=5, pady=2)
        result_frame.columnconfigure(1, weight=1)
        ttk.Label(result_frame, text="Perte (g) :").grid(row=0, column=0, sticky="e")
        self.loss_var = tk.StringVar(value=f"{loss_w:.2f}" if loss_w is not None else '')
        ttk.Label(result_frame, textvariable=self.loss_var).grid(row=0, column=1, sticky="w")
        ttk.Label(result_frame, text="Perte (%) :").grid(row=0, column=2, sticky="e")
        self.loss_pct_var = tk.StringVar(value=f"{loss_pct:.2f}" if loss_pct is not None else '')
        ttk.Label(result_frame, textvariable=self.loss_pct_var).grid(row=0, column=3, sticky="w")
        ttk.Label(result_frame, text="Durée :").grid(row=0, column=4, sticky="e")
        duration_str = ''
        if first_issue_time and last_time:
            try:
                dt_issue = datetime.strptime(first_issue_time, "%Y-%m-%d %H:%M:%S")
                dt_ret = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                delta = dt_ret - dt_issue
                hours = delta.total_seconds() / 3600.0
                duration_str = format_duration(hours)
            except Exception:
                duration_str = ''
        self.duration_var = tk.StringVar(value=duration_str)
        ttk.Label(result_frame, textvariable=self.duration_var).grid(row=0, column=5, sticky="w")
        # Boutons de sauvegarde et suppression
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=5)
        self.del_btn = ttk.Button(btn_frame, text="Supprimer l'étape", command=self.delete_step)
        self.del_btn.pack(side='left', padx=5)

        # Verrouille les champs GWT/Scrap s'ils ont déjà des valeurs
        self._lock_issue_gwt_entries()
        self._auto_save_in_progress = False

    def _on_destroy(self, event):
        if event is not None and event.widget is not self:
            return
        self._cancel_auto_save()
        self._clear_tracked_traces()
        for win in list(getattr(self, '_detail_windows', [])):
            try:
                win.destroy()
            except tk.TclError:
                pass
        if hasattr(self, '_detail_windows'):
            self._detail_windows.clear()

    def _track_stringvar(self, var, line=None):
        if var is None:
            return
        trace_id = var.trace_add("write", self._on_tracked_var_write)
        if line is None:
            self._tracked_traces.append((var, trace_id))
        else:
            line.setdefault('trace_ids', []).append((var, trace_id))

    def _remove_line_traces(self, line):
        for var, trace_id in list(line.get('trace_ids', [])):
            try:
                var.trace_remove("write", trace_id)
            except tk.TclError:
                pass
        line['trace_ids'] = []
        for var, trace_id in list(line.get('extra_traces', [])):
            try:
                var.trace_remove("write", trace_id)
            except tk.TclError:
                pass
        line['extra_traces'] = []

    def _clear_tracked_traces(self):
        for var, trace_id in getattr(self, '_tracked_traces', []):
            try:
                var.trace_remove("write", trace_id)
            except tk.TclError:
                pass
        if hasattr(self, '_tracked_traces'):
            self._tracked_traces.clear()
        for line in getattr(self, 'issue_lines', []):
            self._remove_line_traces(line)
        for line in getattr(self, 'delivery_lines', []):
            self._remove_line_traces(line)

    def _on_tracked_var_write(self, *_):
        if self._auto_save_in_progress:
            return
        self._schedule_auto_save()

    def _cancel_auto_save(self):
        if self._auto_save_after_id is not None:
            try:
                self.after_cancel(self._auto_save_after_id)
            except tk.TclError:
                pass
            self._auto_save_after_id = None

    def _schedule_auto_save(self):
        if self._auto_save_in_progress:
            return
        try:
            exists = self.winfo_exists()
        except tk.TclError:
            exists = False
        if not exists:
            return
        self._cancel_auto_save()
        try:
            self._auto_save_after_id = self.after(
                self._auto_save_delay_ms, self._run_auto_save
            )
        except tk.TclError:
            self._auto_save_after_id = None

    def _run_auto_save(self):
        if self._auto_save_after_id is not None:
            self._auto_save_after_id = None
        if self._auto_save_in_progress:
            return
        self._auto_save_in_progress = True
        try:
            self.save_step(silent=True)
        finally:
            self._auto_save_in_progress = False

    def _create_float_field(self, label: str, base_row: int, col_index: int, value, prefix: str = "issue_"):
        """Crée un champ de saisie flottant pour Gwt/Scrap/Stone/Finding."""
        row = base_row
        if prefix == "return_":
            row = base_row
        ttk.Label(self, text=f"{prefix.split('_')[0].capitalize()} {label} :").grid(row=row, column=0 if prefix == 'issue_' else 2, sticky="e")
        var = tk.StringVar(value=str(value) if value is not None else '')
        entry = ttk.Entry(self, textvariable=var, width=10)
        entry.grid(row=row, column=1 if prefix == 'issue_' else 3, sticky="w", padx=5, pady=2)
        # stocke la variable et l'entry dans des attributs dynamiques
        setattr(self, f"{prefix}{label.lower()}_var", var)
        setattr(self, f"{prefix}{label.lower()}_entry", entry)

    def _add_issue_line(self, issue=None):
        """Ajoute une ligne d'issue dans l'UI."""
        if issue is None:
            issue = {}
        row = len(self.issue_lines)
        dt_var = tk.StringVar(
            value=issue.get('issue_time')
            or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        gwt_display, gwt_formula, gwt_value = self._prepare_initial_numeric(
            issue.get('gwt'), issue.get('gwt_formula')
        )
        scrap_display, scrap_formula, scrap_value = self._prepare_initial_numeric(
            issue.get('scrap'), issue.get('scrap_formula')
        )
        stone_display, stone_formula, stone_value = self._prepare_initial_numeric(
            issue.get('stone'), issue.get('stone_formula')
        )
        find_display, find_formula, find_value = self._prepare_initial_numeric(
            issue.get('finding'), issue.get('finding_formula')
        )
        gwt_var = tk.StringVar(value=gwt_display)
        scrap_var = tk.StringVar(value=scrap_display)
        stone_var = tk.StringVar(value=stone_display)
        find_var = tk.StringVar(value=find_display)
        desc_var = tk.StringVar(value=issue.get('description') or '')

        line_frame = ttk.Frame(self.issue_container)
        line_frame.grid(row=row, column=0, sticky="ew", pady=2)
        line_frame.columnconfigure(1, weight=1)

        ttk.Label(line_frame, text="Date/Heure :").grid(row=0, column=0, sticky="e")
        dt_entry = ttk.Entry(line_frame, textvariable=dt_var, width=19, state='disabled')
        dt_entry.grid(row=0, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="GWT :").grid(row=1, column=0, sticky="e")
        gwt_entry = ttk.Entry(line_frame, textvariable=gwt_var, width=10)
        gwt_entry.grid(row=1, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="Scrap :").grid(row=2, column=0, sticky="e")
        scrap_entry = ttk.Entry(line_frame, textvariable=scrap_var, width=10)
        scrap_entry.grid(row=2, column=1, sticky="w", padx=2, pady=2)

        stone_label = ttk.Label(line_frame, text="Stone :")
        stone_label.grid(row=3, column=0, sticky="e")
        stone_entry = ttk.Entry(line_frame, textvariable=stone_var, width=10)
        stone_entry.grid(row=3, column=1, sticky="w", padx=2, pady=2)

        find_label = ttk.Label(line_frame, text="Find :")
        find_label.grid(row=4, column=0, sticky="e")
        find_entry = ttk.Entry(line_frame, textvariable=find_var, width=10)
        find_entry.grid(row=4, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="Description :").grid(row=5, column=0, sticky="e")
        desc_entry = ttk.Entry(line_frame, textvariable=desc_var)
        desc_entry.grid(row=5, column=1, sticky="ew", padx=2, pady=2)

        line = {
            'time_var': dt_var,
            'gwt_var': gwt_var,
            'scrap_var': scrap_var,
            'stone_var': stone_var,
            'find_var': find_var,
            'desc_var': desc_var,
            'frame': line_frame,
            'gwt_formula': gwt_formula,
            'scrap_formula': scrap_formula,
            'stone_formula': stone_formula,
            'finding_formula': find_formula,
            'gwt_display': gwt_display,
            'scrap_display': scrap_display,
            'stone_display': stone_display,
            'find_display': find_display,
            'gwt_value': gwt_value,
            'scrap_value': scrap_value,
            'stone_value': stone_value,
            'find_value': find_value,
            'trace_ids': [],
        }
        line['widgets'] = [line_frame]
        stone_label.configure(cursor="hand2")
        stone_label.bind(
            "<Double-Button-1>",
            lambda _e, ln=line: self._open_detail_manager(
                'stone',
                ln,
                'stone',
                'stone_formula',
                "Stone (issue)",
            ),
        )
        find_label.configure(cursor="hand2")
        find_label.bind(
            "<Double-Button-1>",
            lambda _e, ln=line: self._open_detail_manager(
                'findings',
                ln,
                'find',
                'finding_formula',
                "Findings (issue)",
            ),
        )

        self._setup_numeric_entry(gwt_entry, line, 'gwt')
        self._setup_numeric_entry(scrap_entry, line, 'scrap')
        self._setup_numeric_entry(stone_entry, line, 'stone')
        self._setup_numeric_entry(find_entry, line, 'find', formula_key='finding_formula')

        desc_entry.bind("<FocusOut>", lambda e: self._schedule_auto_save(), add="+")
        self._track_stringvar(desc_var, line=line)
        for numeric_var in (gwt_var, scrap_var, stone_var, find_var):
            self._track_stringvar(numeric_var, line=line)

        del_btn = ttk.Button(
            line_frame,
            text="Supprimer",
            command=lambda l=line: self._remove_issue_line(l),
        )
        del_btn.grid(row=6, column=0, columnspan=2, pady=2, sticky="w")

        self.issue_lines.append(line)
        if hasattr(self, 'add_issue_btn'):
            self._update_issue_add_button()

    def bind_drag_callbacks(self, start_cb, motion_cb, release_cb):
        """Assigne les callbacks de glisser-déposer au header de l'étape."""

        for widget in (getattr(self, 'drag_handle', None), getattr(self, 'title_label', None)):
            if widget is None:
                continue
            widget.bind("<ButtonPress-1>", start_cb, add="+")
            widget.bind("<B1-Motion>", motion_cb, add="+")
            widget.bind("<ButtonRelease-1>", release_cb, add="+")

    def _remove_issue_line(self, line):
        self._remove_line_traces(line)
        for w in line['widgets']:
            w.destroy()
        if line in self.issue_lines:
            self.issue_lines.remove(line)
        for idx, l in enumerate(self.issue_lines):
            l['frame'].grid_configure(row=idx)
        self._update_issue_add_button()
        self._schedule_auto_save()

    def _update_issue_add_button(self):
        row = len(self.issue_lines)
        self.add_issue_btn.grid(row=row, column=0, pady=2, sticky="w")

    def _format_number(self, value):
        if value is None:
            return ''
        try:
            number = float(value)
        except (TypeError, ValueError):
            return ''
        text = f"{number:.6f}".rstrip('0').rstrip('.')
        if text in {'', '-'}:
            text = '0'
        if text == '-0':
            text = '0'
        return text

    def _prepare_initial_numeric(self, value, formula):
        normalized = None
        numeric_value = None
        if formula:
            try:
                numeric_value, normalized = self._evaluate_numeric_expression(formula)
            except ValueError:
                normalized = None
                numeric_value = None
        if numeric_value is None and value is not None:
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                numeric_value = None
        display = self._format_number(numeric_value)
        if normalized is None and display:
            normalized = f"={display}"
        return display, normalized, numeric_value

    def _evaluate_numeric_expression(self, text: str):
        if text is None:
            return None, None
        expression = str(text).strip()
        if not expression:
            return None, None
        if expression.startswith('='):
            expression = expression[1:].strip()
        if not expression:
            raise ValueError("Expression numérique vide.")
        try:
            tree = ast.parse(expression, mode='eval')
        except SyntaxError as exc:
            raise ValueError(
                "Expression numérique invalide. Utilisez uniquement les nombres ainsi que + ou -."
            ) from exc

        invalid_message = (
            "Expression numérique invalide. Utilisez uniquement les nombres ainsi que + ou -."
        )

        def _validate(node):
            if isinstance(node, ast.Expression):
                _validate(node.body)
            elif isinstance(node, ast.BinOp):
                if not isinstance(node.op, (ast.Add, ast.Sub)):
                    raise ValueError(invalid_message)
                _validate(node.left)
                _validate(node.right)
            elif isinstance(node, ast.UnaryOp):
                if not isinstance(node.op, (ast.UAdd, ast.USub)):
                    raise ValueError(invalid_message)
                _validate(node.operand)
            elif isinstance(node, ast.Constant):
                if not isinstance(node.value, (int, float)):
                    raise ValueError(invalid_message)
            elif isinstance(node, ast.Num):
                if not isinstance(node.n, (int, float)):
                    raise ValueError(invalid_message)
            else:
                raise ValueError(invalid_message)

        def _eval(node):
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            if isinstance(node, ast.BinOp):
                left = _eval(node.left)
                right = _eval(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                return left - right
            if isinstance(node, ast.UnaryOp):
                operand = _eval(node.operand)
                if isinstance(node.op, ast.UAdd):
                    return +operand
                return -operand
            if isinstance(node, ast.Constant):
                return float(node.value)
            if isinstance(node, ast.Num):
                return float(node.n)
            raise ValueError(invalid_message)

        _validate(tree)
        result = float(_eval(tree))
        if hasattr(ast, "unparse"):
            normalized_expr = ast.unparse(tree.body).replace(' ', '')
        else:
            normalized_expr = self._unparse_numeric_expression(tree.body)
        return result, f"={normalized_expr}"

    @staticmethod
    def _unparse_numeric_expression(node):
        """Retourne une version normalisée d'un AST restreint aux additions/soustractions."""

        def _build(current):
            if isinstance(current, ast.BinOp):
                left_txt, left_prec = _build(current.left)
                right_txt, right_prec = _build(current.right)
                operator = '+' if isinstance(current.op, ast.Add) else '-'
                precedence = 1
                if left_prec < precedence:
                    left_txt = f"({left_txt})"
                if (
                    right_prec < precedence
                    or (
                        isinstance(current.op, ast.Sub)
                        and isinstance(current.right, ast.BinOp)
                    )
                ):
                    right_txt = f"({right_txt})"
                return f"{left_txt}{operator}{right_txt}", precedence
            if isinstance(current, ast.UnaryOp):
                operand_txt, operand_prec = _build(current.operand)
                precedence = 2
                if operand_prec < precedence:
                    operand_txt = f"({operand_txt})"
                symbol = '+' if isinstance(current.op, ast.UAdd) else '-'
                return f"{symbol}{operand_txt}", precedence
            if isinstance(current, ast.Constant):
                value = current.value
            elif isinstance(current, ast.Num):
                value = current.n
            else:
                raise ValueError(
                    "Expression numérique invalide. Utilisez uniquement les nombres ainsi que + ou -."
                )
            if isinstance(value, float):
                text = repr(float(value))
            else:
                text = str(value)
            return text, 3

        normalized, _ = _build(node)
        return normalized.replace(' ', '')

    def _evaluate_numeric_field(
        self,
        line,
        field,
        *,
        formula_key=None,
        raise_on_error=False,
        show_error=False,
        finalize_ui=True,
    ):
        # clés
        var_key = f"{field}_var"
        entry_key = f"{field}_entry"
        display_key = f"{field}_display"
        formula_store = formula_key or f"{field}_formula"

        var = line.get(var_key)
        entry = line.get(entry_key)
        if var is None or entry is None:
            return None, None

        entry_state = str(entry.cget('state') or 'normal')
        stored_formula = (line.get(formula_store) or '').strip()

        # Source du texte
        text = (var.get() or '').strip() if (entry_state == 'normal' or not stored_formula) else stored_formula

        # Rien à évaluer → garder éditable, ne rien effacer
        if not text:
            line[formula_store] = None
            if finalize_ui:
                entry.config(state='normal')
            return None, None

        # Essayer d'évaluer
        try:
            value, formula = self._evaluate_numeric_expression(text)
        except ValueError as exc:
            if finalize_ui:
                if show_error:
                    from tkinter import messagebox
                    messagebox.showerror("Expression invalide", str(exc))
                entry.config(state='normal')
                entry.after(0, entry.focus_set)
            if raise_on_error:
                raise
            return None, None

        formatted = self._format_number(value)

        # 🔴 Pas un nombre → ne pas figer, afficher la formule si rien n’est à l’écran
        if formatted == '':
            if finalize_ui:
                entry.config(state='normal')
                if not (var.get() or '').strip():
                    var.set(formula or '')
            line[formula_store] = formula
            return None, formula

        # ✅ Nombre → figer + mémoriser
        line[formula_store] = formula
        line[display_key] = formatted
        line[f"{field}_value"] = value
        if finalize_ui:
            var.set(formatted)
            entry.config(state='readonly')
        return value, formula


    def _on_numeric_double_click(self, line, field, formula_key=None):
        entry = line.get(f"{field}_entry")
        var = line.get(f"{field}_var")
        if entry is None or var is None:
            return
        entry.config(state='normal')
        key = formula_key or f"{field}_formula"
        formula = line.get(key)
        if formula:
            var.set(formula)
        else:
            current = line.get(f"{field}_display") or var.get().strip()
            if current:
                var.set(f"={current}")
        entry.focus_set()
        try:
            entry.selection_range(0, tk.END)
        except tk.TclError:
            pass

    def _setup_numeric_entry(self, entry, line, field, formula_key=None):
        entry._is_numeric_field = True
        entry_key = f"{field}_entry"
        line[entry_key] = entry
        entry.bind(
            "<FocusOut>",
            lambda e, ln=line, f=field, fk=formula_key: self._on_numeric_focus_out(
                ln, f, fk
            ),
        )
        entry.bind(
            "<Double-Button-1>",
            lambda e, ln=line, f=field, fk=formula_key: self._on_numeric_double_click(
                ln, f, fk
            ),
        )
        display_value = line.get(f"{field}_display")
        if display_value:
            entry.config(state='readonly')

    def _on_numeric_focus_out(self, line, field, formula_key=None):
        try:
            self._evaluate_numeric_field(
                line,
                field,
                formula_key=formula_key,
                raise_on_error=True,
                show_error=True,
            )
        except ValueError:
            return
        self._schedule_auto_save()

    def _get_worker_names(self):
        names = []
        if self.data_manager is not None:
            try:
                workers = self.data_manager.list_workers()
            except Exception:
                workers = []
            for worker in workers:
                try:
                    name = worker[1]
                except Exception:
                    name = None
                if name and name not in names:
                    names.append(name)
        if not names and callable(self.worker_fetcher):
            try:
                fetched = list(self.worker_fetcher())
            except Exception:
                fetched = []
            names.extend(self._normalize_worker_names(fetched))
        return names

    def _normalize_worker_names(self, worker_names):
        normalized = []
        if worker_names is None:
            return normalized
        for item in worker_names:
            if isinstance(item, str):
                name = item
            else:
                try:
                    name = item[1]
                except Exception:
                    continue
            name = str(name).strip()
            if not name or name in normalized:
                continue
            normalized.append(name)
        return normalized

    def _build_worker_values(self, worker_names):
        values = ['']
        for name in worker_names:
            if name and name not in values:
                values.append(name)
        current = self.worker_var.get()
        if current and current not in values:
            values.append(current)
        return values

    def _refresh_worker_values(self):
        self.update_worker_list()

    def update_worker_list(self, worker_names=None):
        if worker_names is None:
            worker_names = self._get_worker_names()
        normalized = self._normalize_worker_names(worker_names)
        self._worker_names_cache = normalized
        if hasattr(self, 'worker_entry'):
            values = self._build_worker_values(normalized)
            self.worker_entry['values'] = values
            current = self.worker_var.get()
            if current:
                try:
                    self.worker_entry.set(current)
                except tk.TclError:
                    pass
            else:
                try:
                    self.worker_entry.set('')
                except tk.TclError:
                    pass

    def _lock_issue_gwt_entries(self):
        for line in self.issue_lines:
            entry = line.get('gwt_entry')
            var = line.get('gwt_var')
            if entry is None or var is None:
                continue
            # Ne pas toucher aux entrées actuellement en édition : sinon
            # l'auto-sauvegarde les repasse en lecture seule pendant la saisie.
            state = str(entry.cget('state') or 'normal')
            if state == 'normal':
                continue

            has_value = bool(
                (line.get('gwt_display') or '').strip()
                or (line.get('gwt_formula') or '').strip()
                or var.get().strip()
            )
            if has_value:
                entry.config(state='readonly')
            else:
                entry.config(state='normal')

    def _add_delivery_line(self, delivery=None):
        """Ajoute une ligne de livraison dans l'UI."""
        if delivery is None:
            delivery = {}
        row = len(self.delivery_lines)
        dt_var = tk.StringVar(
            value=delivery.get('delivery_time')
            or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        desc_var = tk.StringVar(value=delivery.get('description') or '')
        gwt_display, gwt_formula, gwt_value = self._prepare_initial_numeric(
            delivery.get('gwt'), delivery.get('gwt_formula')
        )
        scrap_display, scrap_formula, scrap_value = self._prepare_initial_numeric(
            delivery.get('scrap'), delivery.get('scrap_formula')
        )
        stone_display, stone_formula, stone_value = self._prepare_initial_numeric(
            delivery.get('return_stone'), delivery.get('return_stone_formula')
        )
        find_display, find_formula, find_value = self._prepare_initial_numeric(
            delivery.get('return_finding'), delivery.get('return_finding_formula')
        )
        gwt_var = tk.StringVar(value=gwt_display)
        scrap_var = tk.StringVar(value=scrap_display)
        stone_var = tk.StringVar(value=stone_display)
        find_var = tk.StringVar(value=find_display)

        line_frame = ttk.Frame(self.delivery_container)
        line_frame.grid(row=row, column=0, sticky="ew", pady=2)
        line_frame.columnconfigure(1, weight=1)
        line_frame.columnconfigure(2, weight=0)

        ttk.Label(line_frame, text="Date/Heure :").grid(row=0, column=0, sticky="e")
        dt_entry = ttk.Entry(line_frame, textvariable=dt_var, width=19, state='disabled')
        dt_entry.grid(row=0, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="GWT :").grid(row=1, column=0, sticky="e")
        gwt_entry = ttk.Entry(line_frame, textvariable=gwt_var, width=10)
        gwt_entry.grid(row=1, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="Scrap :").grid(row=2, column=0, sticky="e")
        scrap_entry = ttk.Entry(line_frame, textvariable=scrap_var, width=10)
        scrap_entry.grid(row=2, column=1, sticky="w", padx=2, pady=2)

        # Préparer l'objet ligne avant de créer des callbacks qui y font référence
        line = {}

        scrap_confirm_btn = ttk.Button(
            line_frame,
            text="Livrer",
            width=8,
            command=lambda l=line: self._confirm_scrap_delivery(l),
        )
        scrap_confirm_btn.grid(row=2, column=2, padx=4, pady=2, sticky="w")
        scrap_status = ttk.Label(line_frame, text="Livrée", foreground="red")
        scrap_status.grid(row=2, column=2, padx=4, pady=2, sticky="w")

        stone_label = ttk.Label(line_frame, text="Return Stone :")
        stone_label.grid(row=3, column=0, sticky="e")
        stone_entry = ttk.Entry(line_frame, textvariable=stone_var, width=10)
        stone_entry.grid(row=3, column=1, sticky="w", padx=2, pady=2)

        return_find_label = ttk.Label(line_frame, text="Return Findings :")
        return_find_label.grid(row=4, column=0, sticky="e")
        find_entry = ttk.Entry(line_frame, textvariable=find_var, width=10)
        find_entry.grid(row=4, column=1, sticky="w", padx=2, pady=2)

        ttk.Label(line_frame, text="Description :").grid(row=5, column=0, sticky="e")
        desc_entry = ttk.Entry(line_frame, textvariable=desc_var)
        desc_entry.grid(row=5, column=1, sticky="ew", padx=2, pady=2)

        line.update({
            'time_var': dt_var,
            'desc_var': desc_var,
            'gwt_var': gwt_var,
            'scrap_var': scrap_var,
            'stone_var': stone_var,
            'find_var': find_var,
            'frame': line_frame,
            'gwt_formula': gwt_formula,
            'scrap_formula': scrap_formula,
            'return_stone_formula': stone_formula,
            'return_finding_formula': find_formula,
            'gwt_display': gwt_display,
            'scrap_display': scrap_display,
            'stone_display': stone_display,
            'find_display': find_display,
            'gwt_value': gwt_value,
            'scrap_value': scrap_value,
            'stone_value': stone_value,
            'find_value': find_value,
            'trace_ids': [],
            'extra_traces': [],
            'scrap_delivered': bool(delivery.get('scrap_delivered')),
            'has_scrap_input': bool(scrap_display),
            'scrap_confirm_btn': scrap_confirm_btn,
            'scrap_status_label': scrap_status,
        })
        line['widgets'] = [line_frame]
        stone_label.configure(cursor="hand2")
        stone_label.bind(
            "<Double-Button-1>",
            lambda _e, ln=line: self._open_detail_manager(
                'return_stone',
                ln,
                'stone',
                'return_stone_formula',
                "Return Stone",
            ),
        )

        self._setup_numeric_entry(gwt_entry, line, 'gwt')
        self._setup_numeric_entry(scrap_entry, line, 'scrap')
        self._setup_numeric_entry(stone_entry, line, 'stone', formula_key='return_stone_formula')
        self._setup_numeric_entry(find_entry, line, 'find', formula_key='return_finding_formula')

        desc_entry.bind("<FocusOut>", lambda e: self._schedule_auto_save(), add="+")
        self._track_stringvar(desc_var, line=line)
        for numeric_var in (gwt_var, scrap_var, stone_var, find_var):
            self._track_stringvar(numeric_var, line=line)
        scrap_trace_id = scrap_var.trace_add(
            "write", lambda *_: self._on_delivery_scrap_change(line)
        )
        line['extra_traces'].append((scrap_var, scrap_trace_id))
        self._update_scrap_delivery_status(line)

        del_btn = ttk.Button(
            line_frame,
            text="Supprimer",
            command=lambda l=line: self._remove_delivery_line(l),
        )
        del_btn.grid(row=6, column=0, columnspan=2, pady=2, sticky="w")

        self.delivery_lines.append(line)
        if hasattr(self, 'add_delivery_btn'):
            self._update_add_button_position()

    def _remove_delivery_line(self, line):
        """Supprime une ligne de livraison de l'UI."""
        self._remove_line_traces(line)
        for w in line['widgets']:
            w.destroy()
        if line in self.delivery_lines:
            self.delivery_lines.remove(line)
        for idx, l in enumerate(self.delivery_lines):
            l['frame'].grid_configure(row=idx)
        self._update_add_button_position()
        self._schedule_auto_save()

    def _update_add_button_position(self):
        row = len(self.delivery_lines)
        self.add_delivery_btn.grid(row=row, column=0, columnspan=3, pady=2, sticky="w")

    def _on_delivery_scrap_change(self, line, *_):
        var = line.get('scrap_var')
        if var is None:
            return
        text = (var.get() or '').strip()
        line['has_scrap_input'] = bool(text)
        self._update_scrap_delivery_status(line)

    def _update_scrap_delivery_status(self, line):
        delivered = bool(line.get('scrap_delivered'))
        has_input = bool(line.get('has_scrap_input'))
        btn = line.get('scrap_confirm_btn')
        status = line.get('scrap_status_label')
        if delivered:
            if btn is not None:
                try:
                    btn.grid_remove()
                except tk.TclError:
                    pass
            if status is not None:
                try:
                    status.grid()
                except tk.TclError:
                    pass
        else:
            if status is not None:
                try:
                    status.grid_remove()
                except tk.TclError:
                    pass
            if btn is not None:
                try:
                    if has_input:
                        btn.grid()
                    else:
                        btn.grid_remove()
                except tk.TclError:
                    pass

    def _open_detail_manager(self, detail_type, line, field, formula_key, display_name):
        def _on_detail_update(_type, total):
            self._apply_detail_total(line, field, formula_key, total)

        window = DetailManageWindow(
            self,
            self.step_id,
            detail_type,
            self.data_manager,
            _on_detail_update,
            display_name=display_name,
        )
        self._detail_windows.append(window)
        window.bind(
            "<Destroy>",
            lambda _e, w=window: self._on_detail_window_destroy(w),
            add="+",
        )

    def _on_detail_window_destroy(self, window):
        try:
            self._detail_windows.remove(window)
        except ValueError:
            pass

    def _apply_detail_total(self, line, field, formula_key, total):
        var = line.get(f"{field}_var")
        entry = line.get(f"{field}_entry")
        if var is None or entry is None:
            return
        numeric_value = None
        formatted = ''
        if total not in (None, ''):
            try:
                numeric_value = float(total)
            except (TypeError, ValueError):
                numeric_value = None
            if numeric_value is not None:
                formatted = self._format_number(numeric_value)
        var.set(formatted)
        try:
            entry.config(state='readonly' if formatted else 'normal')
        except tk.TclError:
            pass
        display_key = f"{field}_display"
        line[display_key] = formatted
        value_key = f"{field}_value"
        line[value_key] = numeric_value
        if formula_key:
            line[formula_key] = None
        self._schedule_auto_save()

    def _confirm_scrap_delivery(self, line):
        if line.get('scrap_delivered'):
            return
        if not bool(line.get('has_scrap_input')):
            messagebox.showwarning(
                "Confirmation",
                "Veuillez saisir une valeur de scrap avant de confirmer la livraison.",
            )
            return
        try:
            scrap_value, _ = self._evaluate_numeric_field(
                line,
                'scrap',
                raise_on_error=True,
                show_error=True,
            )
        except ValueError:
            return
        if not messagebox.askyesno("Confirmation", "Ce morceau est-il livré ?"):
            return
        line['scrap_delivered'] = True
        if scrap_value is not None:
            line['scrap_value'] = scrap_value
        self._update_scrap_delivery_status(line)
        try:
            self.save_step(silent=True)
        except Exception:
            self._schedule_auto_save()

    def save_step(self, silent=False):
        """Sauvegarde les données de l'étape avec option silencieuse."""
        self._cancel_auto_save()
        previous_state = self._auto_save_in_progress
        self._auto_save_in_progress = True
        try:
            saved = self._persist_step(silent=silent)
        finally:
            self._auto_save_in_progress = previous_state
        if saved and not silent:
            messagebox.showinfo("Sauvegarde", f"Étape '{self.name}' mise à jour.")
        return saved

    def _persist_step(self, silent=False):
        current_workers = self._get_worker_names()
        self.update_worker_list(current_workers)
        if not getattr(self, '_worker_names_cache', []) and not silent:
            messagebox.showinfo(
                "Ouvriers manquants",
                "Aucun ouvrier n'est disponible. Utilisez le gestionnaire des ouvriers pour en créer.",
            )
        worker = self.worker_var.get().strip() or None

        for i in self.data_manager.get_issues(self.step_id):
            self.data_manager.delete_issue(i[0])

        issue_gwt_total = issue_scrap_total = issue_stone_total = issue_find_total = 0.0
        first_time = None
        first_desc = None
        has_issue = False
        for line in self.issue_lines:
            dt = line['time_var'].get().strip() or None
            try:
                gwt, gwt_formula = self._evaluate_numeric_field(
                    line,
                    'gwt',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                scrap, scrap_formula = self._evaluate_numeric_field(
                    line,
                    'scrap',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                stone, stone_formula = self._evaluate_numeric_field(
                    line,
                    'stone',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                find, find_formula = self._evaluate_numeric_field(
                    line,
                    'find',
                    formula_key='finding_formula',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
            except ValueError:
                return False
            desc = line['desc_var'].get().strip() or None
            if not any(v is not None for v in [dt, gwt, scrap, stone, find, desc]):
                continue
            stone_grams = carats_to_grams(stone)
            self.data_manager.add_issue(
                self.step_id,
                dt,
                gwt,
                scrap,
                stone_grams,
                find,
                description=desc,
                gwt_formula=gwt_formula,
                scrap_formula=scrap_formula,
                stone_formula=stone_formula,
                finding_formula=find_formula,
            )
            issue_gwt_total += gwt or 0.0
            issue_scrap_total += scrap or 0.0
            issue_stone_total += stone_grams or 0.0
            issue_find_total += find or 0.0
            if dt and (first_time is None or dt < first_time):
                first_time = dt
                first_desc = desc
            has_issue = True

        self.data_manager.update_step(
            self.step_id,
            worker=worker,
            issue_time=first_time,
            issue_desc=first_desc,
            issue_gwt=issue_gwt_total if has_issue else None,
            issue_scrap=issue_scrap_total if has_issue else None,
            issue_stone=issue_stone_total if has_issue else None,
            issue_finding=issue_find_total if has_issue else None,
        )

        for d in self.data_manager.get_deliveries(self.step_id):
            self.data_manager.delete_delivery(d[0])

        delivery_gwt_total = delivery_scrap_total = delivery_stone_total = delivery_find_total = 0.0
        last_time = None
        has_delivery = False
        for line in self.delivery_lines:
            dt = line['time_var'].get().strip() or None
            desc = line['desc_var'].get().strip() or None
            try:
                gwt, gwt_formula = self._evaluate_numeric_field(
                    line,
                    'gwt',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                scrap, scrap_formula = self._evaluate_numeric_field(
                    line,
                    'scrap',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                stone, stone_formula = self._evaluate_numeric_field(
                    line,
                    'stone',
                    formula_key='return_stone_formula',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
                find, find_formula = self._evaluate_numeric_field(
                    line,
                    'find',
                    formula_key='return_finding_formula',
                    raise_on_error=True,
                    show_error=not silent,
                    finalize_ui=not silent,
                )
            except ValueError:
                return False
            if not any(v is not None for v in [dt, gwt, scrap, stone, find, desc]):
                continue
            stone_grams = carats_to_grams(stone)
            self.data_manager.add_delivery(
                self.step_id,
                dt,
                gwt,
                scrap,
                stone_grams,
                find,
                description=desc,
                gwt_formula=gwt_formula,
                scrap_formula=scrap_formula,
                return_stone_formula=stone_formula,
                return_finding_formula=find_formula,
                scrap_delivered=int(bool(line.get('scrap_delivered'))),
            )
            delivery_gwt_total += gwt or 0.0
            delivery_scrap_total += scrap or 0.0
            delivery_stone_total += stone_grams or 0.0
            delivery_find_total += find or 0.0
            if dt and (last_time is None or dt > last_time):
                last_time = dt
            has_delivery = True

        loss_w = loss_pct = None
        if has_delivery:
            loss_w, loss_pct = compute_loss(
                issue_gwt_total,
                issue_scrap_total,
                delivery_gwt_total,
                delivery_scrap_total,
                delivery_stone_total,
                delivery_find_total,
                issue_finding=issue_find_total,
                issue_stone=issue_stone_total,
            )

        self.data_manager.update_step(
            self.step_id,
            return_time=last_time,
            return_gwt=delivery_gwt_total if has_delivery else None,
            return_scrap=delivery_scrap_total if has_delivery else None,
            return_stone=delivery_stone_total if has_delivery else None,
            return_finding=delivery_find_total if has_delivery else None,
            loss_weight=loss_w,
            loss_pct=loss_pct,
        )

        if loss_w is not None:
            self.loss_var.set(f"{loss_w:.2f}")
        else:
            self.loss_var.set('')
        if loss_pct is not None:
            self.loss_pct_var.set(f"{loss_pct:.2f}")
        else:
            self.loss_pct_var.set('')

        dur = ''
        if first_time and last_time:
            try:
                dt_i = datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
                dt_r = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                diff = dt_r - dt_i
                dur_hours = diff.total_seconds() / 3600.0
                dur = format_duration(dur_hours)
            except Exception:
                dur = ''
        self.duration_var.set(dur)

        self._lock_issue_gwt_entries()
        self.on_update_callback(self.job_id)
        return True

    def delete_step(self):
        """Supprime cette étape après confirmation."""
        if not messagebox.askyesno("Suppression", f"Supprimer l'étape '{self.name}' ?"):
            return
        self.data_manager.delete_step(self.step_id)
        # Recharge la liste des étapes depuis la base
        if self.reload_callback:
            self.reload_callback()
        self.on_update_callback(self.job_id)


class DetailManageWindow(tk.Toplevel):
    """Fenêtre de gestion des détails (Findings ou Stone) d'une étape."""

    def __init__(self, parent, step_id: int, detail_type: str, data_manager: DataManager, update_callback, display_name: Optional[str] = None):
        super().__init__(parent)
        self.step_id = step_id
        self.detail_type = detail_type
        self.data_manager = data_manager
        self.update_callback = update_callback
        self.display_name = display_name or detail_type
        self.title(f"Détails {self.display_name}")
        self.geometry("400x300")

        ttk.Label(self, text=f"Détails pour {self.display_name}").pack(pady=5)
        cols = ("Description", "Pcs", "GWT")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="browse")
        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, anchor="center", minwidth=50, width=120)
        self.tree.pack(fill="both", expand=True, padx=5, pady=5)
        self.tree.bind("<Delete>", self.delete_selected)
        self.tree.bind("<Double-1>", self.start_edit)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)

        ttk.Button(btn_frame, text="Ajouter une ligne", command=self.add_line).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Enregistrer", command=self.save).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Effacer la ligne", command=self.delete_selected_row).pack(side="left", padx=5)


        self.load_details()
        self.deleted_detail_ids = []


    def _format_number(self, value):
        if value in (None, ""):
            return ""
        try:
            number = float(value)
        except (TypeError, ValueError):
            return ""
        text = f"{number:.6f}".rstrip("0").rstrip(".")
        return text or "0"


    def load_details(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        rows = self.data_manager.get_details(self.step_id, self.detail_type)
        for row in rows:
            detail_id, desc, pcs, gwt = row
            display_gwt = gwt
            if self.detail_type in {"stone", "return_stone"}:
                display_gwt = grams_to_carats(gwt)
            formatted_gwt = self._format_number(display_gwt)
            pcs_display = "" if pcs in (None, "") else str(pcs)
            self.tree.insert(
                "",
                "end",
                iid=detail_id,
                values=(desc or "", pcs_display, formatted_gwt),
            )
        self.update_total()

    def add_row(self):
        self.tree.insert("", "end", values=("", "", ""))

    def start_edit(self, event):
        item = self.tree.identify_row(event.y)
        column = self.tree.identify_column(event.x)
        if not item or column == "#0":
            return
        x, y, w, h = self.tree.bbox(item, column)
        value = self.tree.set(item, column)
        self.edit_var = tk.StringVar(value=value)
        if hasattr(self, "edit_entry") and self.edit_entry:
            self.edit_entry.destroy()
        self.edit_entry = tk.Entry(self.tree, textvariable=self.edit_var)
        self.edit_entry.place(x=x, y=y, width=w, height=h)
        self.edit_entry.focus()
        self.edit_entry.bind("<Return>", lambda e: self.save_edit(item, column))
        self.edit_entry.bind("<FocusOut>", lambda e: self.save_edit(item, column))

    def save_edit(self, item, column):
        if not hasattr(self, "edit_entry") or not self.edit_entry:
            return
        new_val = self.edit_var.get()
        self.tree.set(item, column, new_val)
        self.edit_entry.destroy()
        self.edit_entry = None


    def save(self):
        # 🔁 Supprimer tous les détails supprimés par l’utilisateur
        for detail_id in self.deleted_detail_ids:
            self.data_manager.delete_detail(detail_id)

        # Réinitialiser la liste après suppression
        self.deleted_detail_ids = []

        for item in self.tree.get_children():
            desc, pcs, gwt = self.tree.item(item, "values")
            gwt_val = (gwt or "").strip() if isinstance(gwt, str) else gwt
            if gwt_val in ("", None):
                continue
            try:
                gwt_f = float(gwt_val)
            except (TypeError, ValueError):
                continue
            pcs_val = (pcs or "").strip() if isinstance(pcs, str) else pcs
            try:
                pcs_i = int(pcs_val) if pcs_val not in ("", None) else None
            except (TypeError, ValueError):
                pcs_i = None
            desc_str = desc.strip() if isinstance(desc, str) else None
            stored_gwt = gwt_f
            if self.detail_type in {"stone", "return_stone"}:
                stored_gwt = carats_to_grams(gwt_f)
            data = {
                "description": desc_str or None,
                "pcs": pcs_i,
                "gwt": stored_gwt,
            }
            try:
                detail_id = int(item)
            except (TypeError, ValueError):
                detail_id = None
            if detail_id:
                self.data_manager.update_detail_and_sync(detail_id, data)
            else:
                self.data_manager.add_detail(
                    self.step_id,
                    self.detail_type,
                    data["description"],
                    data["pcs"],
                    data["gwt"],
                )

        self.load_details()
        messagebox.showinfo("Enregistrer", "Détails sauvegardés")



    def delete_selected_row(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Aucune ligne sélectionnée", "Veuillez sélectionner une ligne à effacer.")
            return

        for item in selected:
            try:
                detail_id = int(item)
                self.deleted_detail_ids.append(detail_id)
            except ValueError:
                pass  # L'élément n'a pas encore d'ID (nouvelle ligne)
            self.tree.delete(item)


    def add_line(self):
        """Ajoute une ligne vide dans le tableau."""
        self.tree.insert("", "end", values=("", "", ""))


    def delete_selected(self, event=None):
        item = self.tree.focus()
        if not item:
            return
        if not messagebox.askyesno("Suppression", "Supprimer la ligne sélectionnée ?"):
            return
        self.tree.delete(item)

    def update_total(self):
        total = self.data_manager.get_detail_sum(self.step_id, self.detail_type)
        field_mapping = {
            "findings": "issue_finding",
            "stone": "issue_stone",
            "return_findings": "return_finding",
            "return_stone": "return_stone",
        }
        field_name = field_mapping.get(self.detail_type)
        if field_name:
            self.data_manager.update_step(self.step_id, **{field_name: total})
        display_total = total
        if self.detail_type in {"stone", "return_stone"}:
            display_total = grams_to_carats(total)
        if self.update_callback:
            self.update_callback(self.detail_type, display_total)

    def close(self):
        self.save()
        self.destroy()


class JobWindow(tk.Toplevel):
    """Fenêtre de détail d'un job JBA."""

    def __init__(self, master, job_id: int, data_manager: DataManager):
        super().__init__(master)
        self.job_id = job_id
        self.data_manager = data_manager
        self._worker_fetcher = getattr(self.master, 'get_worker_names', lambda: [])
        self.title(f"Détails du travail #{job_id}")
        self.geometry("900x700")
        # Récupère les données du job et des étapes
        job, steps = self.data_manager.get_job(job_id)
        if not job:
            messagebox.showerror("Erreur", "Job introuvable.")
            self.destroy()
            return
        # Affichage des informations générales
        ttk.Label(self, text=f"Référence : {job[1]}", font=(DEFAULT_FONT_FAMILY, 14, "bold")).pack(pady=5, anchor='w')
        ttk.Label(self, text=f"Description : {job[2] or ''}").pack(pady=2, anchor='w')
        # Zone d'affichage du résumé et du solde de scrap
        summary_frame = ttk.Frame(self)
        summary_frame.pack(pady=2, anchor='w', fill='x')
        self.summary_label = ttk.Label(summary_frame, text="", foreground="#007AFF")
        self.summary_label.pack(side='left')
        self.scrap_label = ttk.Label(summary_frame, text="", foreground="red")
        self.scrap_label.pack(side='right')
        # Boutons d'action relatifs au job
        button_frame = ttk.Frame(self)
        button_frame.pack(pady=5, anchor="w")
        self.add_step_btn = ttk.Button(
            button_frame,
            text="Ajouter une étape",
            command=self.add_step_dialog,
        )
        self.add_step_btn.pack(side="left")
        self.toggle_finish_btn = ttk.Button(
            button_frame,
            text="Marquer comme fini",
            command=self.toggle_job_finished,
        )
        self.toggle_finish_btn.pack(side="left", padx=(5, 0))
        self.save_btn = ttk.Button(
            button_frame,
            text="Enregistrer",
            command=lambda: self.save_all_steps(show_message=False),
        )
        self.save_btn.pack(side="left", padx=(5, 0))
        self._update_finish_button_label(job[3])

        # Conteneur pour les étapes avec scrollbar
        steps_container = ttk.Frame(self)
        steps_container.pack(fill="both", expand=True)
        canvas = tk.Canvas(steps_container, background=self.cget('bg'), highlightthickness=0)
        scrollbar = ttk.Scrollbar(steps_container, orient="vertical", command=canvas.yview)
        self.steps_canvas = canvas
        self.steps_frame = ttk.Frame(canvas)
        self.steps_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        self._steps_frame_window_id = canvas.create_window(
            (0, 0),
            window=self.steps_frame,
            anchor="n",
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(
                self._steps_frame_window_id,
                width=e.width,
                anchor="n",
            ),
        )
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        # Création des StepFrame
        self.step_frames = []
        self._drag_data = {}
        self.load_steps(steps)
        # Mise à jour initiale du résumé
        self.update_summary()
        if hasattr(self.master, 'register_job_window'):
            self.master.register_job_window(self)
        self.bind("<Destroy>", self._on_destroy, add="+")
        self.protocol("WM_DELETE_WINDOW", self._on_close_request)

    def _on_destroy(self, event):
        if event.widget is self and hasattr(self.master, 'unregister_job_window'):
            self.master.unregister_job_window(self)

    def save_all_steps(self, show_message=True):
        saved_any = False
        errors = []
        for frame in getattr(self, 'step_frames', []):
            try:
                if frame.save_step(silent=True):
                    saved_any = True
            except Exception as exc:
                errors.append(str(exc))
        if errors:
            messagebox.showerror(
                "Erreur",
                "Impossible d'enregistrer toutes les étapes :\n" + "\n".join(errors),
            )
            return False
        self.update_summary()
        if show_message:
            message = (
                "Modifications enregistrées." if saved_any else "Aucune modification détectée."
            )
            messagebox.showinfo("Enregistrement", message)
        return saved_any

    def _on_close_request(self):
        try:
            self.save_all_steps(show_message=False)
        finally:
            self.destroy()

    def on_step_update(self, job_id):
        """Callback appelé lorsqu'une étape est modifiée."""
        # Recalcule les métriques du job
        self.data_manager.recalc_job_metrics(job_id)
        # Rafraîchit le résumé
        self.update_summary()
        self._update_finish_button_label()
        # Met à jour la liste des jobs sur la fenêtre principale si elle existe
        if hasattr(self.master, 'refresh_job_list'):
            self.master.refresh_job_list()

    def load_steps(self, steps):
        """Charge les StepFrame dans l'interface."""
        # Clear existing frames
        for frame in getattr(self, 'step_frames', []):
            frame.destroy()
        self.step_frames = []
        self._drag_data = {}
        for step in steps:
            sf = StepFrame(
                self.steps_frame,
                step,
                self.data_manager,
                self.on_step_update,
                self.reload_steps,
                worker_fetcher=self.get_worker_names,
            )
            sf.pack(fill="x", pady=5, padx=5)
            sf.bind_drag_callbacks(
                lambda e, frame=sf: self._on_step_drag_start(e, frame),
                lambda e, frame=sf: self._on_step_drag_motion(e, frame),
                lambda e, frame=sf: self._on_step_drag_release(e, frame),
            )
            self.step_frames.append(sf)
        self.steps_frame.update_idletasks()
        self.on_worker_list_changed()

    def reload_steps(self):
        """Recharge les étapes depuis la base et met à jour l'affichage."""
        _job, steps = self.data_manager.get_job(self.job_id)
        self.load_steps(steps)

    def _compute_drop_index(self, relative_y: float) -> int:
        for idx, frame in enumerate(self.step_frames):
            midpoint = frame.winfo_y() + frame.winfo_height() / 2
            if relative_y < midpoint:
                return idx
        return len(self.step_frames)

    def _repack_step_frames(self):
        for frame in self.step_frames:
            frame.pack_forget()
            frame.pack(fill="x", pady=5, padx=5)
        self.steps_frame.update_idletasks()

    def _persist_step_order(self):
        ordered_ids = [frame.step_id for frame in self.step_frames]
        self.data_manager.update_step_order(self.job_id, ordered_ids)
        self.on_step_update(self.job_id)

    def _on_step_drag_start(self, event, step_frame):
        if not hasattr(self, 'steps_canvas'):
            return
        self.steps_canvas.configure(cursor="fleur")
        self._drag_data = {
            "frame": step_frame,
            "start_index": self.step_frames.index(step_frame),
            "reordered": False,
        }

    def _on_step_drag_motion(self, event, step_frame):
        data = getattr(self, '_drag_data', None)
        if not data or data.get("frame") is not step_frame:
            return
        if not hasattr(self, 'steps_frame'):
            return
        self.steps_frame.update_idletasks()
        relative_y = event.y_root - self.steps_frame.winfo_rooty()
        drop_index = self._compute_drop_index(relative_y)
        current_index = self.step_frames.index(step_frame)
        if drop_index == current_index or drop_index == current_index + 1:
            return
        new_index = drop_index
        if drop_index == len(self.step_frames):
            new_index = len(self.step_frames) - 1
        elif drop_index > current_index:
            new_index = drop_index - 1
        if new_index < 0:
            new_index = 0
        frame = self.step_frames.pop(current_index)
        if new_index > len(self.step_frames):
            new_index = len(self.step_frames)
        self.step_frames.insert(new_index, frame)
        self._repack_step_frames()
        data["reordered"] = True

    def _on_step_drag_release(self, event, step_frame):
        data = getattr(self, '_drag_data', None)
        if not data or data.get("frame") is not step_frame:
            return
        try:
            self._on_step_drag_motion(event, step_frame)
        except Exception:
            pass
        if hasattr(self, 'steps_canvas'):
            self.steps_canvas.configure(cursor="")
        reordered = data.get("reordered") or (
            self.step_frames.index(step_frame) != data.get("start_index")
        )
        self._drag_data = {}
        if reordered:
            self._persist_step_order()

    def get_worker_names(self):
        try:
            names = list(self._worker_fetcher())
        except Exception:
            names = []
        return names

    def on_worker_list_changed(self, worker_names=None):
        if worker_names is None:
            worker_names = self.get_worker_names()
        for frame in getattr(self, 'step_frames', []):
            if hasattr(frame, 'update_worker_list'):
                frame.update_worker_list(worker_names)

    def _update_finish_button_label(self, status=None):
        if not hasattr(self, 'toggle_finish_btn'):
            return
        if status is None:
            job, _steps = self.data_manager.get_job(self.job_id)
            status = job[3] if job else None
        label = "Marquer comme fini"
        if isinstance(status, str) and status.strip().lower() in {"fini", "terminé"}:
            label = "Annuler fini"
        self.toggle_finish_btn.config(text=label)

    def toggle_job_finished(self):
        job, _steps = self.data_manager.get_job(self.job_id)
        if not job:
            messagebox.showerror("Erreur", "Job introuvable.")
            return

        current_status = job[3]
        normalized = (
            current_status.strip().lower()
            if isinstance(current_status, str)
            else ""
        )
        mark_finished = normalized not in {"fini", "terminé"}
        new_status = "Fini" if mark_finished else None

        try:
            updated = self.data_manager.update_job_fields(
                self.job_id,
                status=new_status,
            )
            if not updated:
                raise RuntimeError("Aucune mise à jour effectuée.")
            self.data_manager.recalc_job_metrics(self.job_id)
        except Exception as exc:
            messagebox.showerror(
                "Erreur",
                f"Impossible de mettre à jour le statut : {exc}",
            )
            return

        self.update_summary()
        if hasattr(self.master, 'refresh_job_list'):
            self.master.refresh_job_list()

        job_after, _ = self.data_manager.get_job(self.job_id)
        status_after = job_after[3] if job_after else new_status
        self._update_finish_button_label(status_after)

    def add_step_dialog(self):
        """Fenêtre pour choisir et ajouter une nouvelle étape."""
        # Limite à 15 étapes
        if len(self.step_frames) >= 15:
            messagebox.showwarning("Limite", "Vous avez atteint le nombre maximal d'étapes (15).")
            return
        top = tk.Toplevel(self)
        top.title("Ajouter une étape")
        ttk.Label(top, text="Choisissez le nom de l'étape :").pack(padx=10, pady=5)
        options = [
            'Jobwork Pret',
            'Tige',
            'Limage/ Montage',
            'Papier',
            'Sertissage',
            'Correction',
            'Vérification qualité',
            'Polissage',
            'Polissage Final'
        ]
        var = tk.StringVar(value=options[0])
        combo = ttk.Combobox(top, values=options, textvariable=var, state='readonly')
        combo.pack(padx=10, pady=5)
        def add():
            name = var.get()
            self.data_manager.add_step(self.job_id, name)
            # Recharge les étapes depuis la base
            self.reload_steps()
            # Mettre à jour la fenêtre principale
            if hasattr(self.master, 'refresh_job_list'):
                self.master.refresh_job_list()
            top.destroy()
        ttk.Button(top, text="Ajouter", command=add).pack(side='left', padx=10, pady=10)
        ttk.Button(top, text="Annuler", command=top.destroy).pack(side='right', padx=10, pady=10)
        top.grab_set()

    def update_summary(self):
        summary = self.data_manager.get_job_summary(self.job_id)
        if not summary:
            return
        # Durée formatée en jours, heures et minutes
        hours = summary['duration_hours']
        if hours and hours > 0:
            days = int(hours // 24)
            remaining_hours = hours - days * 24
            # Utilise format_duration pour afficher les heures restantes
            duration_str = f"{days} j {format_duration(remaining_hours)}"
        else:
            duration_str = "-"
        # Travail fini
        final_work = summary.get('final_work')
        final_work_str = f"{final_work:.2f} g" if final_work not in (None, 0, '') else "-"
        loss_total = summary.get('total_loss') or 0.0
        loss_pct = summary.get('total_loss_pct') if summary.get('total_loss_pct') is not None else 0.0
        text = (
            f"Travail fini : {final_work_str} | "
            f"Durée : {duration_str} | "
            f"Perte totale : {loss_total:.2f} g | "
            f"Perte % : {loss_pct:.2f}%"
        )
        self.summary_label.config(text=text)
        self.scrap_label.config(text=f"Morceaux à livrer : {summary['piece_total']:.2f} g")


class WorkerManagerDialog(tk.Toplevel):
    """Fenêtre de gestion des ouvriers enregistrés."""

    def __init__(
        self,
        master,
        data_manager: DataManager,
        on_workers_changed=None,
        initial_workers=None,
    ):
        super().__init__(master)
        self.data_manager = data_manager
        self.on_workers_changed = on_workers_changed
        self.title("Gestion des ouvriers")
        self.resizable(False, False)
        if hasattr(master, 'cget'):
            self.configure(bg=master.cget('bg'))
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.close)

        self.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            self,
            columns=("ordre", "name"),
            show="headings",
            selectmode="browse",
            height=12,
        )
        self.tree.heading("ordre", text="Ordre")
        self.tree.heading("name", text="Nom")
        self.tree.column("ordre", width=80, anchor="center")
        self.tree.column("name", width=220, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew", padx=10, pady=(10, 5))
        self.tree.bind("<Delete>", lambda _event: self.delete_worker())

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        btn_frame.columnconfigure(2, weight=1)

        add_btn = ttk.Button(btn_frame, text="+", width=3, command=self.add_worker)
        add_btn.grid(row=0, column=0, padx=(0, 5))

        del_btn = ttk.Button(btn_frame, text="Supprimer", command=self.delete_worker)
        del_btn.grid(row=0, column=1, padx=(0, 5))

        close_btn = ttk.Button(btn_frame, text="Fermer", command=self.close)
        close_btn.grid(row=0, column=3, sticky="e")

        self.refresh_workers(initial_workers)

    def refresh_workers(self, workers=None):
        """Rafraîchit l'affichage de la liste des ouvriers."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        if workers is None:
            try:
                workers = self.data_manager.list_workers()
            except Exception as exc:
                messagebox.showerror("Erreur", f"Impossible de récupérer les ouvriers : {exc}")
                return
        for worker_id, name, ordre in workers:
            ordre_txt = ordre if ordre is not None else ""
            self.tree.insert('', 'end', iid=str(worker_id), values=(ordre_txt, name))

    def add_worker(self):
        """Ajoute un nouvel ouvrier via une boîte de dialogue."""
        name = simple_input(self, "Nouvel ouvrier", "Nom de l'ouvrier :")
        if name is None:
            return
        try:
            self.data_manager.add_worker(name)
        except Exception as exc:
            messagebox.showerror("Erreur", f"Impossible d'ajouter l'ouvrier : {exc}")
            return
        self.refresh_workers()
        self._notify_change()

    def delete_worker(self):
        """Supprime l'ouvrier sélectionné."""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Suppression", "Veuillez sélectionner un ouvrier à supprimer.")
            return
        item_id = selection[0]
        values = self.tree.item(item_id, 'values')
        name = values[1] if len(values) > 1 else ""
        if not messagebox.askyesno("Suppression", f"Supprimer l'ouvrier {name} ?"):
            return
        try:
            self.data_manager.delete_worker(int(item_id))
        except Exception as exc:
            messagebox.showerror("Erreur", f"Impossible de supprimer l'ouvrier : {exc}")
            return
        self.refresh_workers()
        self._notify_change()

    def _notify_change(self):
        if callable(self.on_workers_changed):
            self.on_workers_changed()

    def close(self):
        self.grab_release()
        self.destroy()


class MainApplication(tk.Tk):
    """Fenêtre principale de l'application."""

    def __init__(self, data_manager):
        super().__init__()
        global DEFAULT_FONT_FAMILY
        # Applique la police par défaut à toute l'interface
        default_font = tkfont.nametofont("TkDefaultFont")
        available_fonts = set()
        try:
            available_fonts = {name.lower() for name in tkfont.families()}
        except Exception:
            # Certaines plateformes (ou anciennes versions de Tk) peuvent lever
            # une exception lors de l'interrogation des polices disponibles.
            available_fonts = set()

        preferred_fonts = [DEFAULT_FONT_FAMILY, *FALLBACK_FONT_FAMILIES]

        current_default_family = default_font.actual().get("family")
        if current_default_family:
            preferred_fonts.append(current_default_family)

        chosen_family = None
        for family in preferred_fonts:
            if not family:
                continue
            # Tk ne fait pas de distinction de casse sur les noms de police.
            if family.lower() in available_fonts:
                chosen_family = family
                break

        if chosen_family is None:
            # Aucun des noms préférés n'est disponible ; on laisse la police
            # par défaut fournie par Tk.
            chosen_family = current_default_family

        if chosen_family:
            try:
                default_font.configure(family=chosen_family)
            except tk.TclError:
                # Sur certains environnements (ex : Windows 7), tenter de
                # configurer une police inexistante peut échouer. Dans ce cas,
                # on conserve simplement la police par défaut de Tk.
                chosen_family = current_default_family

        # Mémorise la police retenue pour les autres widgets créés plus tard.
        if chosen_family:
            DEFAULT_FONT_FAMILY = chosen_family

        self.option_add("*Font", default_font)
        self.title("Gestion de production bijouterie")
        self.geometry("900x600")
        # Application d'un style moderne avec des couleurs douces et un thème clair
        style = ttk.Style()
        # Choisit un thème clair si disponible
        try:
            if 'clam' in style.theme_names():
                style.theme_use('clam')
        except Exception:
            pass
        # Définition de couleurs pour l'interface
        primary_bg = '#F5F7FA'  # couleur d'arrière-plan principale
        secondary_bg = '#FFFFFF'  # pour les cadres et widgets
        accent_color = '#007AFF'  # couleur d'accent (bleu)
        # Configure les styles par défaut
        style.configure('TFrame', background=primary_bg)
        style.configure('TLabel', background=primary_bg, foreground='#333333', font=(DEFAULT_FONT_FAMILY, 10))
        style.configure('TButton', font=(DEFAULT_FONT_FAMILY, 10), padding=(8, 4))
        style.configure('TEntry', font=(DEFAULT_FONT_FAMILY, 10))
        style.configure('TLabelframe', background=secondary_bg)
        style.configure('TLabelframe.Label', background=secondary_bg, font=(DEFAULT_FONT_FAMILY, 11, 'bold'), foreground='#222222')
        # Style pour Treeview (tableaux)
        style.configure('Treeview', background=secondary_bg, fieldbackground=secondary_bg, font=(DEFAULT_FONT_FAMILY, 9), rowheight=24)
        style.configure('Treeview.Heading', background=primary_bg, foreground=accent_color, font=(DEFAULT_FONT_FAMILY, 10, 'bold'))
        style.map('Treeview', background=[('selected', '#D0E7FF')], foreground=[('selected', '#000000')])
        # Couleur de fond de la fenêtre principale
        self.configure(bg=primary_bg)
        # Initialisation du gestionnaire de données
        self.data_manager = data_manager
        self.job_windows = []
        self._worker_dialog = None
        self._worker_names = []
        self._refresh_worker_cache()
        # Barre d'outils
        toolbar = ttk.Frame(self)
        toolbar.pack(fill="x", pady=5)
        add_btn = ttk.Button(toolbar, text="Nouveau JBA", command=self.new_job)
        add_btn.pack(side="left", padx=5)
        del_btn = ttk.Button(toolbar, text="Supprimer", command=self.delete_job)
        del_btn.pack(side="left", padx=5)
        refresh_btn = ttk.Button(toolbar, text="Actualiser", command=self.refresh_job_list)
        refresh_btn.pack(side="left", padx=5)
        edit_btn = ttk.Button(toolbar, text="Modifier JBA", command=self.edit_job)
        edit_btn.pack(side="left", padx=5)
        reset_btn = ttk.Button(toolbar, text="Réinitialiser", command=self.reset_application)
        reset_btn.pack(side="left", padx=5)
        summary_btn = ttk.Button(toolbar, text="Bilan mensuel", command=self.open_summary_menu)
        summary_btn.pack(side="left", padx=5)
        worker_btn = ttk.Button(toolbar, text="WORKER", command=self.open_worker_manager)
        worker_btn.pack(side="left", padx=5)
        # Tableau des jobs
        columns = ("Ref", "Description", "Étape", "Ouvrier", "Début", "Fin", "Perte %")
        self.tree = ttk.Treeview(self, columns=columns, show='headings', selectmode='browse')
        for col in columns:
            self.tree.heading(col, text=col)
            # largeur
            if col in ("Ref", "Étape"):
                width = 80
            elif col in ("Ouvrier", "Description"):
                width = 150
            else:
                width = 100
            self.tree.column(col, minwidth=60, width=width)
        self.tree.pack(fill="both", expand=True)
        self.tree.bind("<Double-1>", self.on_double_click)
        # Chargement initial



        self.refresh_job_list()

    def _refresh_worker_cache(self):
        try:
            workers = self.data_manager.list_workers()
        except Exception:
            self._worker_names = []
            return []
        self._worker_names = [name for _wid, name, _ordre in workers]
        return workers

    def get_worker_names(self):
        return list(getattr(self, '_worker_names', []))

    def open_worker_manager(self):
        if self._worker_dialog and self._worker_dialog.winfo_exists():
            self._worker_dialog.deiconify()
            self._worker_dialog.lift()
            self._worker_dialog.focus_set()
            return
        workers = self._refresh_worker_cache()
        self._worker_dialog = WorkerManagerDialog(
            self,
            self.data_manager,
            on_workers_changed=self.on_workers_changed,
            initial_workers=workers,
        )
        self._worker_dialog.bind(
            "<Destroy>",
            lambda _event: setattr(self, "_worker_dialog", None),
            add="+",
        )

    def register_job_window(self, window):
        if window not in self.job_windows:
            self.job_windows.append(window)
            if hasattr(window, 'on_worker_list_changed'):
                window.on_worker_list_changed(self.get_worker_names())

    def unregister_job_window(self, window):
        if window in self.job_windows:
            self.job_windows.remove(window)

    def _notify_worker_windows(self, worker_names):
        for win in list(self.job_windows):
            if not win.winfo_exists():
                self.job_windows.remove(win)
                continue
            if hasattr(win, 'on_worker_list_changed'):
                win.on_worker_list_changed(worker_names)

    def on_workers_changed(self):
        workers = self._refresh_worker_cache()
        if self._worker_dialog and self._worker_dialog.winfo_exists():
            self._worker_dialog.refresh_workers(workers)
        self._notify_worker_windows(self.get_worker_names())

    def refresh_job_list(self):
        """Recharge la liste des jobs dans le tableau."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        try:
            jobs = self.data_manager.list_jobs()
        except Exception as exc:
            messagebox.showerror("Erreur", f"Impossible de récupérer les travaux : {exc}")
            return
        for row in jobs:
            job_id, ref, desc, status, start_time, end_time, total_loss, total_loss_pct = row
            status_text = status or ''
            status_normalized = status_text.strip().lower()
            is_finished = status_normalized in {"fini", "terminé"}

            current = self.data_manager.get_current_step(job_id)
            last_completed = None
            if not current and not is_finished:
                last_completed = self.data_manager.get_last_completed_step(job_id)

            worker = ''
            step_name = ''
            start_fmt = start_time or ''
            end_fmt = end_time or ''
            bg_color = '#FFFFFF'

            if current:
                step_name = current[1] or ''
                worker = current[2] or ''
                step_issue_time = current[3]
                start_fmt = step_issue_time or ''
                end_fmt = ''
                name_norm = step_name.lower()
                bg_color = STEP_COLOR_MAP.get(name_norm, '#FFFFFF')
            elif is_finished:
                summary = self.data_manager.get_job_summary(job_id)
                worker = summary['worker'] if summary else ''
                display_status = status_text.strip() or 'Fini'
                step_name = display_status
                bg_color = '#008000'
            elif last_completed:
                step_name = last_completed[1] or ''
                worker = last_completed[2] or ''
                end_fmt = last_completed[3] or ''
                name_norm = step_name.lower()
                bg_color = STEP_COLOR_MAP.get(name_norm, '#FFFFFF')
            else:
                summary = self.data_manager.get_job_summary(job_id)
                worker = summary['worker'] if summary else ''
                step_name = 'À valider'

            start_fmt = start_fmt or ''
            end_fmt = end_fmt or ''
            loss_pct = f"{total_loss_pct:.2f}%" if total_loss_pct is not None else ''

            self.tree.insert(
                '',
                'end',
                iid=job_id,
                values=(ref, desc or '', step_name, worker, start_fmt, end_fmt, loss_pct),
                tags=(f"job_{job_id}",),
            )
            self.tree.tag_configure(f"job_{job_id}", background=bg_color)

    def new_job(self):
        """Crée un nouveau job et ouvre sa fenêtre de détail."""
        ref = self.data_manager.generate_new_ref()
        desc = simple_input(self, "Nouvelle référence", f"Créer {ref} ?\nEntrez une description :")
        if desc is None:
            return
        try:
            job_id = self.data_manager.add_job(ref, desc)
        except Exception as exc:
            messagebox.showerror("Erreur", f"Impossible de créer le travail : {exc}")
            return
        self.refresh_job_list()
        JobWindow(self, job_id, self.data_manager)

    def on_double_click(self, event):
        """Ouvre la fenêtre de détail du job sélectionné lors d'un double-clic."""
        item = self.tree.focus()
        if item:
            job_id = int(item)
            JobWindow(self, job_id, self.data_manager)

    def delete_job(self):
        """Supprime le job sélectionné après confirmation."""
        item = self.tree.focus()
        if not item:
            messagebox.showwarning("Suppression", "Veuillez sélectionner un job à supprimer.")
            return
        job_id = int(item)
        values = self.tree.item(item, 'values')
        ref = values[0]
        if messagebox.askyesno("Suppression", f"Supprimer le travail {ref} ? Toutes les données seront perdues." ):
            self.data_manager.delete_job(job_id)
            self.refresh_job_list()

    def reset_application(self):
        """Réinitialise la base de données après confirmation de l'utilisateur."""
        if not messagebox.askyesno("Réinitialisation", "Réinitialiser la base de données ?"):
            return
        name = simple_input(self, "Nom du reset", "Nom pour cette réinitialisation :")
        if not name:
            return
        try:
            archived = self.data_manager.reset_database(name)
        except Exception as exc:
            messagebox.showerror("Réinitialisation", f"Impossible de réinitialiser : {exc}")
            return

        if not archived:
            messagebox.showinfo("Réinitialisation", "Aucun travail fini à archiver.")
        else:
            messagebox.showinfo(
                "Réinitialisation",
                f"{archived} travail(s) fini(s) archivé(s).",
            )

        self.refresh_job_list()
        self.on_workers_changed()

    def export_excel(self):
        """Demande un emplacement de fichier et exporte les comptes terminés."""
        if pd is None:
            messagebox.showerror("Erreur", "La bibliothèque pandas est requise pour l'export.")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            title="Exporter les JBA terminés"
        )
        if file_path:
            try:
                self.data_manager.export_jobs_to_excel(file_path)
                messagebox.showinfo("Export", f"Export réalisé avec succès :\n{file_path}")
            except Exception as e:
                messagebox.showerror("Erreur", f"Impossible d'exporter : {e}")

    def open_summary_menu(self):
        """Ouvre un menu permettant de choisir le type de bilan."""
        win = tk.Toplevel(self)
        win.title("Bilan")
        win.transient(self)
        win.grab_set()
        ttk.Button(
            win,
            text="Bilan mensuel",
            command=lambda: [win.destroy(), self.show_monthly_summary()],
        ).pack(fill="x", padx=20, pady=10)
        ttk.Button(
            win,
            text="Historique",
            command=lambda: [win.destroy(), self.show_history()],
        ).pack(fill="x", padx=20, pady=10)

    def show_history(self):
        """Affiche l'historique des bilans."""
        resets = self.data_manager.list_resets()
        if not resets:
            messagebox.showinfo("Historique", "Aucun historique disponible pour le moment.")
            return

        win = tk.Toplevel(self)
        win.title("Historique des bilans")
        listbox = tk.Listbox(win)
        for reset in resets:
            listbox.insert(tk.END, reset.get("name", ""))
        listbox.pack(fill="both", expand=True)

        def on_open(event):
            selection = listbox.curselection()
            if not selection:
                return
            reset = resets[selection[0]]
            self.open_reset_summary(reset)

        listbox.bind("<Double-1>", on_open)

    def open_reset_summary(self, reset):
        """Ouvre le bilan mensuel pour une base archivée."""
        dm = DataManager(reset.get("path"))
        worker_data = dm.get_worker_monthly_summary()
        monthly_totals = build_monthly_totals(dm.get_monthly_summary())
        display_monthly_summary(
            self,
            worker_data,
            monthly_totals,
            title=f"Bilan – {reset.get('name', '')}",
        )
        ArchivedJobsWindow(
            self,
            dm,
            title=f"Archives – {reset.get('name', '')}",
        )

    def show_monthly_summary(self):
        """Affiche un bilan mensuel détaillé par ouvrier et étape."""
        worker_data = self.data_manager.get_worker_monthly_summary()
        monthly_totals = build_monthly_totals(self.data_manager.get_monthly_summary())
        display_monthly_summary(self, worker_data, monthly_totals, title="Bilan mensuel")

    def edit_job(self):
       item = self.tree.focus()
       if not item:
           messagebox.showwarning("Modification", "Veuillez sélectionner un job à modifier.")
           return

       job_id = int(item)
       values = self.tree.item(item, 'values')
       current_ref = values[0]
       current_desc = values[1]

       # Fenêtre de modification
       top = tk.Toplevel(self)
       top.title("Modifier le job")

       tk.Label(top, text="Référence :").pack(padx=10, pady=5)
       ref_var = tk.StringVar(value=current_ref)
       tk.Entry(top, textvariable=ref_var).pack(padx=10, pady=5)
 
       tk.Label(top, text="Description :").pack(padx=10, pady=5)
       desc_var = tk.StringVar(value=current_desc)
       tk.Entry(top, textvariable=desc_var).pack(padx=10, pady=5)

       def save_modification():
          new_ref = ref_var.get().strip()
          new_desc = desc_var.get().strip()
          if new_ref and new_ref != current_ref:
              self.data_manager.update_job_fields(job_id, ref=new_ref, description=new_desc)
          else:
             self.data_manager.update_job_fields(job_id, description=new_desc)
          top.destroy()
          self.refresh_job_list()

       tk.Button(top, text="Enregistrer", command=save_modification).pack(pady=10)
       top.grab_set()



class ArchivedJobDetailWindow(tk.Toplevel):
    """Affiche les détails d'un job archivé en lecture seule."""

    def __init__(self, master, data_manager: DataManager, job_id: int):
        super().__init__(master)
        self.data_manager = data_manager
        job, steps = self.data_manager.get_job(job_id)
        if not job:
            messagebox.showerror("Archive", "Job introuvable dans l'archive.")
            self.destroy()
            return

        self.title(f"Archive – {job[1]}")
        self.geometry("850x500")
        ttk.Label(self, text=f"Référence : {job[1]}", font=(DEFAULT_FONT_FAMILY, 14, "bold")).pack(pady=5, anchor='w', padx=10)
        ttk.Label(self, text=f"Description : {job[2] or ''}").pack(pady=2, anchor='w', padx=10)
        info_frame = ttk.Frame(self)
        info_frame.pack(fill='x', padx=10, pady=5)
        ttk.Label(info_frame, text=f"Statut : {job[3] or 'En cours'}").pack(side='left')
        if job[6] is not None:
            ttk.Label(info_frame, text=f"Perte totale : {job[6]:.2f} g").pack(side='left', padx=15)
        if job[7] is not None:
            ttk.Label(info_frame, text=f"Perte % : {job[7]:.2f}%").pack(side='left')

        columns = ("Étape", "Ouvrier", "Donné", "Retour", "Perte (g)", "Perte (%)", "Desc. donné", "Desc. retour")
        tree = ttk.Treeview(self, columns=columns, show='headings', selectmode='browse')
        for col in columns:
            anchor = 'center'
            width = 120 if col in {"Desc. donné", "Desc. retour"} else 100
            tree.heading(col, text=col)
            tree.column(col, anchor=anchor, minwidth=80, width=width)

        for step in steps:
            loss_weight = step[16]
            loss_pct = step[17]
            tree.insert(
                '',
                'end',
                values=(
                    step[2],
                    step[3] or '',
                    step[4] or '',
                    step[10] or '',
                    f"{loss_weight:.2f}" if loss_weight is not None else '',
                    f"{loss_pct:.2f}%" if loss_pct is not None else '',
                    step[5] or '',
                    step[11] or '',
                ),
            )

        tree.pack(fill='both', expand=True, padx=10, pady=10)
        ttk.Label(self, text="Lecture seule – aucune modification n'est possible.").pack(pady=(0, 10))


class ArchivedJobsWindow(tk.Toplevel):
    """Fenêtre de consultation d'une base archivée (lecture seule)."""

    def __init__(self, master, data_manager: DataManager, title: str = "Archives JBA"):
        super().__init__(master)
        self.data_manager = data_manager
        self.title(title)
        self.geometry("900x600")
        try:
            self.configure(bg=master.cget('bg'))
        except Exception:
            self.configure(bg='#F5F7FA')

        ttk.Label(
            self,
            text="Consultation des archives en lecture seule.",
            font=(DEFAULT_FONT_FAMILY, 11, "bold"),
        ).pack(padx=10, pady=(10, 5), anchor='w')

        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', pady=5)
        ttk.Button(toolbar, text="Nouveau JBA", state='disabled').pack(side='left', padx=5)
        ttk.Button(toolbar, text="Supprimer", state='disabled').pack(side='left', padx=5)
        ttk.Button(toolbar, text="Modifier JBA", state='disabled').pack(side='left', padx=5)
        refresh_btn = ttk.Button(toolbar, text="Actualiser", command=self.refresh_job_list)
        refresh_btn.pack(side='left', padx=5)
        summary_btn = ttk.Button(toolbar, text="Bilan mensuel", command=self.show_monthly_summary)
        summary_btn.pack(side='left', padx=5)

        columns = ("Ref", "Description", "Étape", "Ouvrier", "Début", "Fin", "Perte %")
        self.tree = ttk.Treeview(self, columns=columns, show='headings', selectmode='browse')
        for col in columns:
            self.tree.heading(col, text=col)
            if col in ("Ref", "Étape"):
                width = 80
            elif col in ("Ouvrier", "Description"):
                width = 150
            else:
                width = 100
            self.tree.column(col, minwidth=60, width=width)
        self.tree.pack(fill='both', expand=True, padx=5, pady=5)
        self.tree.bind("<Double-1>", self.on_double_click)

        self.refresh_job_list()

    def refresh_job_list(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        try:
            jobs = self.data_manager.list_jobs()
        except Exception as exc:
            messagebox.showerror("Archives", f"Impossible de récupérer les travaux : {exc}")
            return

        for row in jobs:
            job_id, ref, desc, status, start_time, end_time, total_loss, total_loss_pct = row
            status_text = status or ''
            status_normalized = status_text.strip().lower()
            is_finished = status_normalized in {"fini", "terminé"}

            current = self.data_manager.get_current_step(job_id)
            last_completed = None
            if not current and not is_finished:
                last_completed = self.data_manager.get_last_completed_step(job_id)

            worker = ''
            step_name = ''
            start_fmt = start_time or ''
            end_fmt = end_time or ''
            bg_color = '#FFFFFF'

            if current:
                step_name = current[1] or ''
                worker = current[2] or ''
                step_issue_time = current[3]
                start_fmt = step_issue_time or ''
                end_fmt = ''
                name_norm = step_name.lower()
                bg_color = STEP_COLOR_MAP.get(name_norm, '#FFFFFF')
            elif is_finished:
                summary = self.data_manager.get_job_summary(job_id)
                worker = summary['worker'] if summary else ''
                display_status = status_text.strip() or 'Fini'
                step_name = display_status
                bg_color = STEP_COLOR_MAP.get('fini', '#008000')
            elif last_completed:
                step_name = last_completed[1] or ''
                worker = last_completed[2] or ''
                end_fmt = last_completed[3] or ''
                name_norm = step_name.lower()
                bg_color = STEP_COLOR_MAP.get(name_norm, '#FFFFFF')
            else:
                summary = self.data_manager.get_job_summary(job_id)
                worker = summary['worker'] if summary else ''
                step_name = 'À valider'

            start_fmt = start_fmt or ''
            end_fmt = end_fmt or ''
            loss_pct = f"{total_loss_pct:.2f}%" if total_loss_pct is not None else ''

            self.tree.insert(
                '',
                'end',
                iid=job_id,
                values=(ref, desc or '', step_name, worker, start_fmt, end_fmt, loss_pct),
                tags=(f"job_{job_id}",),
            )
            self.tree.tag_configure(f"job_{job_id}", background=bg_color)

    def on_double_click(self, event):
        item = self.tree.focus()
        if item:
            job_id = int(item)
            ArchivedJobDetailWindow(self, self.data_manager, job_id)

    def show_monthly_summary(self):
        worker_data = self.data_manager.get_worker_monthly_summary()
        monthly_totals = build_monthly_totals(self.data_manager.get_monthly_summary())
        display_monthly_summary(self, worker_data, monthly_totals, title=f"Bilan – {self.title()}")


def simple_input(master, title, prompt):
    """Fenêtre modale simple pour saisir une valeur textuelle. Retourne None si annulé."""
    top = tk.Toplevel(master)
    top.title(title)
    tk.Label(top, text=prompt).pack(padx=10, pady=10)
    entry_var = tk.StringVar()
    entry = tk.Entry(top, textvariable=entry_var, width=40)
    entry.pack(padx=10, pady=5)
    entry.focus_set()
    result = {'value': None}
    def ok():
        result['value'] = entry_var.get()
        top.destroy()
    def cancel():
        top.destroy()
    btn_frame = tk.Frame(top)
    btn_frame.pack(pady=10)
    tk.Button(btn_frame, text="OK", command=ok).pack(side='left', padx=5)
    tk.Button(btn_frame, text="Annuler", command=cancel).pack(side='left', padx=5)
    top.grab_set()
    master.wait_window(top)
    return result['value']


def build_data_manager():
    """Construit le gestionnaire de données en fonction du mode choisi."""
    if USE_API:
        try:
            from api_client import ApiClient
        except ImportError as exc:  # pragma: no cover - dépend de l'environnement
            raise RuntimeError(
                "Le mode API est activé mais la dépendance ApiClient est indisponible"
            ) from exc
        fallback = DataManager(DB_FILENAME)
        return ApiClient(API_BASE, fallback=fallback)
    return DataManager(DB_FILENAME)


if __name__ == '__main__':
    try:
        data_manager = build_data_manager()
    except Exception as exc:  # pragma: no cover - interface graphique
        print(f"Erreur lors de l'initialisation des données : {exc}", file=sys.stderr)
        sys.exit(1)

    app = MainApplication(data_manager)
    app.mainloop()
