import flet as ft
import flet.canvas as cv
import mysql.connector
import subprocess
import csv
import os
import threading
import time
from datetime import datetime
from typing import Optional

# ─────────────────────────────────────────────
#  THEME TOKENS
# ─────────────────────────────────────────────
BG_DARK   = "#0D0F14"
BG_CARD   = "#13161E"
BG_INPUT  = "#1A1D27"
ACCENT    = "#00E5FF"
ACCENT2   = "#7C3AED"
SUCCESS   = "#10B981"
WARNING   = "#F59E0B"
DANGER    = "#EF4444"
TEXT_PRI  = "#F1F5F9"
TEXT_SEC  = "#94A3B8"
TEXT_MUT  = "#475569"
BORDER    = "#1E2435"


def sp(h=8):
    return ft.Container(height=h)


def glow_border(color=BORDER, width=1):
    return ft.Border.all(width, color)


# ─────────────────────────────────────────────
#  DB CONNECTION MANAGER
# ─────────────────────────────────────────────
class DBConnection:
    def __init__(self):
        self.conn: Optional[mysql.connector.MySQLConnection] = None
        self.config = {}

    def connect(self, host, port, user, password, database=""):
        try:
            cfg = dict(host=host, port=int(port), user=user, password=password)
            if database:
                cfg["database"] = database
            self.conn = mysql.connector.connect(**cfg)
            self.config = cfg
            return True, "Conexión exitosa"
        except Exception as e:
            return False, str(e)

    def disconnect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    @property
    def is_connected(self):
        try:
            return self.conn is not None and self.conn.is_connected()
        except Exception:
            return False

    def execute(self, query, params=None, fetch=False):
        if not self.is_connected:
            return False, "No hay conexión activa", None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params or ())
            if fetch:
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                cursor.close()
                return True, "", (cols, rows)
            self.conn.commit()
            rowcount = cursor.rowcount
            cursor.close()
            return True, f"OK — {rowcount} fila(s) afectada(s)", None
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False, str(e), None

    def execute_script(self, sql_text):
        results = []
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]
        for stmt in statements:
            upper = stmt.upper().lstrip()
            is_fetch = (upper.startswith("SELECT") or
                        upper.startswith("SHOW") or
                        upper.startswith("DESCRIBE"))
            ok, msg, data = self.execute(stmt, fetch=is_fetch)
            results.append((stmt[:80], ok, msg, data))
        return results

    def list_databases(self):
        ok, _, data = self.execute("SHOW DATABASES", fetch=True)
        if ok and data:
            return [r[0] for r in data[1]]
        return []

    def list_tables(self):
        ok, _, data = self.execute("SHOW TABLES", fetch=True)
        if ok and data:
            return [r[0] for r in data[1]]
        return []

    def get_performance_metrics(self):
        metrics = {}
        queries = {
            "questions":    "SHOW GLOBAL STATUS LIKE 'Questions'",
            "connections":  "SHOW GLOBAL STATUS LIKE 'Threads_connected'",
            "slow_queries": "SHOW GLOBAL STATUS LIKE 'Slow_queries'",
            "uptime":       "SHOW GLOBAL STATUS LIKE 'Uptime'",
            "bytes_in":     "SHOW GLOBAL STATUS LIKE 'Bytes_received'",
            "bytes_out":    "SHOW GLOBAL STATUS LIKE 'Bytes_sent'",
        }
        for key, q in queries.items():
            ok, _, data = self.execute(q, fetch=True)
            if ok and data and data[1]:
                try:
                    metrics[key] = float(data[1][0][1])
                except Exception:
                    metrics[key] = 0
        return metrics


db = DBConnection()


# ─────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────
def card(content, padding=20):
    return ft.Container(
        content=content,
        bgcolor=BG_CARD,
        border_radius=12,
        border=glow_border(BORDER),
        padding=padding,
    )


def lbl(text, size=12, color=TEXT_SEC):
    return ft.Text(text, size=size, color=color)


def section_title(icon, title):
    return ft.Row([
        ft.Icon(icon, color=ACCENT, size=20),
        ft.Text(title, size=16, weight=ft.FontWeight.BOLD,
                color=TEXT_PRI, font_family="monospace"),
    ], spacing=10)


def accent_btn(text, on_click, icon=None, color=ACCENT):
    return ft.Container(
        content=ft.Row([
            ft.Icon(icon, color=color, size=18) if icon else ft.Container(),
            ft.Text(text, color=color, size=13, weight=ft.FontWeight.W_500),
        ], spacing=8, alignment=ft.MainAxisAlignment.CENTER),
        bgcolor=f"{color}22",
        border_radius=8,
        border=ft.Border.all(1, color),
        padding=ft.Padding.symmetric(horizontal=16, vertical=10),
        on_click=on_click,
        ink=True,
    )


def danger_btn(text, on_click, icon=None):
    return accent_btn(text, on_click, icon, DANGER)


def success_btn(text, on_click, icon=None):
    return accent_btn(text, on_click, icon, SUCCESS)


def styled_field(label_text, value="", password=False, multiline=False,
                 min_lines=1, max_lines=1, hint="", width=None, expand=False):
    return ft.TextField(
        label=label_text, value=value,
        password=password, can_reveal_password=password,
        multiline=multiline, min_lines=min_lines, max_lines=max_lines,
        hint_text=hint, width=width, expand=expand,
        bgcolor=BG_INPUT,
        border_color=BORDER,
        focused_border_color=ACCENT,
        label_style=ft.TextStyle(color=TEXT_SEC, size=12),
        text_style=ft.TextStyle(color=TEXT_PRI, size=13, font_family="monospace"),
        hint_style=ft.TextStyle(color=TEXT_MUT, size=12),
        border_radius=8,
        cursor_color=ACCENT,
    )


def mk_dropdown(label_text, width=None):
    return ft.Dropdown(
        label=label_text, options=[],
        bgcolor=BG_INPUT, border_color=BORDER,
        focused_border_color=ACCENT,
        label_style=ft.TextStyle(color=TEXT_SEC, size=12),
        text_style=ft.TextStyle(color=TEXT_PRI, size=13),
        border_radius=8, width=width,
    )


# ─────────────────────────────────────────────
#  NOTIFICATION
# ─────────────────────────────────────────────
class Notif:
    def __init__(self, page: ft.Page):
        self.page = page
        self.bar = ft.SnackBar(content=ft.Text(""), bgcolor=BG_CARD)
        self.page.overlay.append(self.bar)

    def show(self, msg, color=SUCCESS):
        icon = (ft.Icons.CHECK_CIRCLE if color == SUCCESS
                else ft.Icons.ERROR if color == DANGER
                else ft.Icons.WARNING)
        self.bar.content = ft.Row(
            [ft.Icon(icon, color=color, size=16),
             ft.Text(msg, color=TEXT_PRI, size=13)], spacing=8)
        self.bar.bgcolor = f"{color}22"
        self.bar.open = True
        self.page.update()


# ─────────────────────────────────────────────
#  TAB 0 – CONEXIÓN
# ─────────────────────────────────────────────
def build_connection_tab(notif: Notif, on_connected):
    host_f = styled_field("Host", "localhost", width=260)
    port_f = styled_field("Puerto", "3306", width=100)
    user_f = styled_field("Usuario", "root", width=200)
    pass_f = styled_field("Contraseña", password=True, width=200)
    db_f   = styled_field("Base de datos (opcional)", width=300)

    dot  = ft.Container(width=8, height=8, bgcolor=DANGER, border_radius=4)
    stat = ft.Text("Desconectado", size=12, color=DANGER, font_family="monospace")
    stat_box = ft.Container(
        content=ft.Row([dot, stat], spacing=8),
        bgcolor=f"{DANGER}18",
        border_radius=8,
        padding=ft.Padding.symmetric(horizontal=14, vertical=8),
    )

    def toggle(e):
        if db.is_connected:
            db.disconnect()
            dot.bgcolor   = DANGER
            stat.value    = "Desconectado"
            stat.color    = DANGER
            stat_box.bgcolor = f"{DANGER}18"
            notif.show("Sesión cerrada", DANGER)
            on_connected(False)
        else:
            ok, msg = db.connect(
                host_f.value, port_f.value,
                user_f.value, pass_f.value, db_f.value)
            if ok:
                dot.bgcolor   = SUCCESS
                stat.value    = f"{user_f.value}@{host_f.value}"
                stat.color    = SUCCESS
                stat_box.bgcolor = f"{SUCCESS}18"
                notif.show(msg, SUCCESS)
                on_connected(True)
            else:
                notif.show(msg, DANGER)
        e.page.update()

    info_rows = [
        ("Motor",   "MariaDB / MySQL"),
        ("Driver",  "mysql-connector-python"),
        ("Charset", "utf8mb4"),
    ]

    return ft.Column([
        section_title(ft.Icons.POWER, "CONEXIÓN A MARIADB"),
        ft.Divider(color=BORDER, height=1),
        sp(10),
        card(ft.Column([
            ft.Row([host_f, port_f], spacing=12),
            ft.Row([user_f, pass_f], spacing=12),
            db_f,
            ft.Row([
                accent_btn("Conectar / Desconectar", toggle,
                           ft.Icons.POWER_SETTINGS_NEW),
                stat_box,
            ], spacing=16),
        ], spacing=14)),
        sp(4),
        card(ft.Column([
            lbl("INFORMACIÓN", size=11, color=ACCENT),
            sp(6),
            *[ft.Row([lbl(k + ":", color=TEXT_MUT),
                      lbl(v, color=TEXT_PRI)], spacing=8)
              for k, v in info_rows],
            sp(4),
            ft.Text(
                "Conecte primero para usar Backup, CSV, Rendimiento y Consola SQL.",
                color=TEXT_SEC, size=12, font_family="monospace"),
        ], spacing=6)),
    ], spacing=12, scroll=ft.ScrollMode.AUTO)


# ─────────────────────────────────────────────
#  TAB 1 – BACKUP & RESTORE
# ─────────────────────────────────────────────
def build_backup_tab(notif: Notif):
    db_sel    = mk_dropdown("Base de datos", width=280)
    path_f    = styled_field("Directorio de destino",
                              os.path.expanduser("~"), width=380)
    restore_f = styled_field("Archivo .sql para restaurar", width=420)
    log_list  = ft.ListView(height=180, spacing=2)
    prog      = ft.ProgressBar(value=0, bgcolor=BORDER, color=ACCENT,
                                border_radius=4, height=6)

    def refresh_dbs(e=None):
        db_sel.options = [ft.dropdown.Option(d) for d in db.list_databases()]
        if e:
            e.page.update()

    def add_log(msg, color=TEXT_SEC):
        ts = datetime.now().strftime("%H:%M:%S")
        log_list.controls.append(
            ft.Text(f"[{ts}] {msg}", size=11, color=color,
                    font_family="monospace", selectable=True))

    def do_backup(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        database = db_sel.value
        if not database:
            notif.show("Seleccione una base de datos", WARNING); return
        dest  = path_f.value.strip() or os.path.expanduser("~")
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = os.path.join(dest, f"backup_{database}_{ts}.sql")
        cfg   = db.config
        add_log(f"Iniciando backup de '{database}'…", ACCENT)
        prog.value = None
        e.page.update()

        def run():
            try:
                cmd = ["mysqldump",
                       f"-h{cfg['host']}", f"-P{cfg['port']}",
                       f"-u{cfg['user']}", f"-p{cfg['password']}",
                       database]
                with open(fname, "w") as f:
                    res = subprocess.run(cmd, stdout=f,
                                         stderr=subprocess.PIPE, text=True)
                if res.returncode == 0:
                    sz = os.path.getsize(fname) / 1024
                    add_log(f"✓ Backup: {fname} ({sz:.1f} KB)", SUCCESS)
                    notif.show(f"Backup: {os.path.basename(fname)}", SUCCESS)
                else:
                    raise RuntimeError(res.stderr)
            except FileNotFoundError:
                add_log("mysqldump no encontrado — exportación alternativa…", WARNING)
                _backup_fallback(database, fname, e, add_log, notif)
            except Exception as ex:
                add_log(f"✗ {ex}", DANGER)
                notif.show("Error en backup", DANGER)
            finally:
                prog.value = 0
                e.page.update()

        threading.Thread(target=run, daemon=True).start()

    def do_restore(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        fpath = restore_f.value.strip()
        if not fpath or not os.path.exists(fpath):
            notif.show("Archivo no encontrado", DANGER); return
        add_log(f"Restaurando '{os.path.basename(fpath)}'…", WARNING)
        prog.value = None
        e.page.update()

        def run():
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    sql = f.read()
                stmts = [s.strip() for s in sql.split(";")
                         if s.strip() and not s.strip().startswith("--")]
                total = max(len(stmts), 1)
                for i, s in enumerate(stmts):
                    db.execute(s)
                    prog.value = (i + 1) / total
                    if i % 30 == 0:
                        e.page.update()
                add_log(f"✓ Restauración completada ({len(stmts)} sentencias)", SUCCESS)
                notif.show("Restauración completada", SUCCESS)
            except Exception as ex:
                add_log(f"✗ {ex}", DANGER)
                notif.show("Error en restauración", DANGER)
            finally:
                prog.value = 0
                e.page.update()

        threading.Thread(target=run, daemon=True).start()

    return ft.Column([
        section_title(ft.Icons.SAVE, "BACKUP & RESTORE"),
        ft.Divider(color=BORDER, height=1),
        ft.Row([
            ft.Container(expand=True, content=card(ft.Column([
                lbl("CREAR BACKUP", size=11, color=SUCCESS),
                sp(6),
                ft.Row([db_sel,
                        accent_btn("↻", refresh_dbs, ft.Icons.REFRESH)],
                       spacing=8),
                path_f,
                success_btn("Iniciar Backup", do_backup, ft.Icons.CLOUD_UPLOAD),
            ], spacing=12))),
            ft.Container(expand=True, content=card(ft.Column([
                lbl("RESTAURAR BACKUP", size=11, color=WARNING),
                sp(6),
                restore_f,
                danger_btn("Restaurar", do_restore, ft.Icons.RESTORE),
                lbl("⚠ Sobreescribirá datos existentes",
                    color=WARNING, size=11),
            ], spacing=12))),
        ], spacing=14),
        card(ft.Column([
            ft.Row([
                lbl("LOG DE OPERACIONES", size=11, color=ACCENT),
                ft.TextButton("Limpiar", on_click=lambda e: (
                    log_list.controls.clear(), e.page.update())),
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            prog,
            ft.Container(
                content=log_list,
                bgcolor=BG_DARK, border_radius=8,
                border=glow_border(BORDER), padding=10, height=200,
            ),
        ], spacing=8)),
    ], spacing=14, scroll=ft.ScrollMode.AUTO)


def _backup_fallback(database, fname, e, add_log, notif):
    try:
        ok, _, data = db.execute("SHOW TABLES", fetch=True)
        lines = [
            f"-- Backup alternativo de {database} — {datetime.now()}\n",
            f"CREATE DATABASE IF NOT EXISTS `{database}`;\n",
            f"USE `{database}`;\n\n",
        ]
        if ok and data:
            for (tbl,) in data[1]:
                ok3, _, cdata = db.execute(
                    f"SHOW CREATE TABLE `{tbl}`", fetch=True)
                ok2, _, tdata = db.execute(
                    f"SELECT * FROM `{tbl}`", fetch=True)
                if ok3 and cdata and cdata[1]:
                    lines += [f"-- Table: {tbl}\n",
                               f"DROP TABLE IF EXISTS `{tbl}`;\n",
                               cdata[1][0][1] + ";\n\n"]
                if ok2 and tdata and tdata[1]:
                    cols = ", ".join(f"`{c}`" for c in tdata[0])
                    for row in tdata[1]:
                        vals = ", ".join(
                            "NULL" if v is None
                            else f"'{str(v).replace(chr(39), chr(39)*2)}'"
                            for v in row)
                        lines.append(
                            f"INSERT INTO `{tbl}` ({cols}) VALUES ({vals});\n")
                    lines.append("\n")
        with open(fname, "w", encoding="utf-8") as f:
            f.writelines(lines)
        sz = os.path.getsize(fname) / 1024
        add_log(f"✓ Backup alt.: {fname} ({sz:.1f} KB)", SUCCESS)
        notif.show("Backup alternativo completado", SUCCESS)
    except Exception as ex2:
        add_log(f"✗ {ex2}", DANGER)
        notif.show("Error en backup alternativo", DANGER)


# ─────────────────────────────────────────────
#  TAB 2 – CSV I/O
# ─────────────────────────────────────────────
def build_csv_tab(notif: Notif):
    tbl_sel  = mk_dropdown("Tabla", width=280)
    exp_path = styled_field("Directorio de exportación",
                             os.path.expanduser("~"), width=360)
    imp_path = styled_field("Archivo CSV para importar", width=360)
    delim_f  = styled_field("Delimitador", ",", width=80)
    row_info = ft.Text("", size=11, color=SUCCESS, font_family="monospace")
    prog     = ft.ProgressBar(value=0, bgcolor=BORDER, color=ACCENT,
                               border_radius=4, height=6)

    preview_tbl = ft.DataTable(
        columns=[ft.DataColumn(ft.Text("—", color=TEXT_MUT))],
        rows=[],
        border=ft.Border.all(1, BORDER),
        border_radius=8,
        heading_row_color=f"{ACCENT}15",
        data_row_color={"hovered": f"{ACCENT}08"},
        column_spacing=20,
    )

    def refresh_tables(e=None):
        tbl_sel.options = [ft.dropdown.Option(t) for t in db.list_tables()]
        if e:
            e.page.update()

    def do_export(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        tbl = tbl_sel.value
        if not tbl:
            notif.show("Seleccione una tabla", WARNING); return
        ok, msg, data = db.execute(f"SELECT * FROM `{tbl}`", fetch=True)
        if not ok:
            notif.show(msg, DANGER); return
        cols, rows = data
        dest  = exp_path.value.strip() or os.path.expanduser("~")
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = os.path.join(dest, f"{tbl}_{ts}.csv")
        try:
            with open(fname, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=delim_f.value or ",")
                w.writerow(cols)
                w.writerows(rows)
            preview_tbl.columns = [
                ft.DataColumn(ft.Text(c, color=ACCENT, size=12,
                                       font_family="monospace"))
                for c in cols]
            preview_tbl.rows = [
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(
                        str(v) if v is not None else "NULL",
                        color=TEXT_PRI, size=11, font_family="monospace"))
                    for v in row])
                for row in rows[:50]]
            row_info.value = f"{len(rows)} filas → {fname}"
            notif.show(f"CSV: {os.path.basename(fname)}", SUCCESS)
        except Exception as ex:
            notif.show(str(ex), DANGER)
        e.page.update()

    def do_import(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        tbl = tbl_sel.value
        if not tbl:
            notif.show("Seleccione tabla destino", WARNING); return
        fpath = imp_path.value.strip()
        if not fpath or not os.path.exists(fpath):
            notif.show("Archivo no encontrado", DANGER); return
        prog.value = None
        e.page.update()

        def run():
            try:
                with open(fpath, newline="", encoding="utf-8") as f:
                    all_rows = list(csv.reader(f,
                                               delimiter=delim_f.value or ","))
                if not all_rows:
                    notif.show("CSV vacío", WARNING); return
                col_names = all_rows[0]
                data_rows = all_rows[1:]
                ph       = ", ".join(["%s"] * len(col_names))
                cols_str = ", ".join(f"`{c}`" for c in col_names)
                q = f"INSERT INTO `{tbl}` ({cols_str}) VALUES ({ph})"
                inserted = 0
                total    = max(len(data_rows), 1)
                for i, row in enumerate(data_rows):
                    ok2, _, _ = db.execute(q, tuple(
                        None if v.upper() == "NULL" else v
                        for v in row))
                    if ok2:
                        inserted += 1
                    prog.value = (i + 1) / total
                    if i % 20 == 0:
                        e.page.update()
                prog.value = 0
                row_info.value = f"{inserted}/{len(data_rows)} filas importadas"
                notif.show(f"Importadas {inserted} filas", SUCCESS)
            except Exception as ex:
                notif.show(str(ex), DANGER)
                prog.value = 0
            e.page.update()

        threading.Thread(target=run, daemon=True).start()

    return ft.Column([
        section_title(ft.Icons.IMPORT_EXPORT, "IMPORTAR / EXPORTAR CSV"),
        ft.Divider(color=BORDER, height=1),
        ft.Row([
            ft.Container(expand=True, content=card(ft.Column([
                lbl("EXPORTAR TABLA", size=11, color=SUCCESS),
                sp(6),
                ft.Row([tbl_sel,
                        accent_btn("↻", refresh_tables, ft.Icons.REFRESH)],
                       spacing=8),
                exp_path, delim_f,
                success_btn("Exportar CSV", do_export, ft.Icons.DOWNLOAD),
            ], spacing=12))),
            ft.Container(expand=True, content=card(ft.Column([
                lbl("IMPORTAR CSV", size=11, color=ACCENT),
                sp(6),
                imp_path,
                lbl("Tabla destino → seleccionar a la izquierda",
                    color=TEXT_MUT),
                accent_btn("Importar CSV", do_import, ft.Icons.UPLOAD),
            ], spacing=12))),
        ], spacing=14),
        card(ft.Column([
            ft.Row([lbl("VISTA PREVIA", size=11, color=ACCENT), row_info],
                   alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            prog,
            ft.Container(
                content=ft.Column(
                    [ft.Row([preview_tbl], scroll=ft.ScrollMode.ALWAYS)],
                    scroll=ft.ScrollMode.AUTO),
                bgcolor=BG_DARK, border_radius=8,
                border=glow_border(BORDER), padding=10, height=280,
            ),
        ], spacing=8)),
    ], spacing=14, scroll=ft.ScrollMode.AUTO)


# ─────────────────────────────────────────────
#  TAB 3 – RENDIMIENTO  (canvas chart)
# ─────────────────────────────────────────────
CHART_W       = 860
CHART_H       = 260
MAX_PTS       = 40
SERIES_COLORS = [ACCENT, SUCCESS, WARNING]
SERIES_LABELS = ["Queries totales", "Conexiones activas", "Slow queries"]
SERIES_KEYS   = ["questions", "connections", "slow_queries"]


def build_perf_tab(notif: Notif, page: ft.Page):
    history    = {k: [] for k in SERIES_KEYS}
    running    = {"v": False}
    interval_f = styled_field("Intervalo (seg)", "3", width=110)

    # ── Canvas chart ───────────────────────────────────────
    chart_canvas = cv.Canvas(shapes=[], width=CHART_W, height=CHART_H)

    def _redraw():
        shapes = []
        # grid
        for i in range(1, 5):
            y = int(CHART_H * i / 4)
            shapes.append(cv.Line(0, y, CHART_W, y,
                                   paint=ft.Paint(color=f"{TEXT_MUT}35",
                                                   stroke_width=1)))
        all_vals = [v for k in SERIES_KEYS for v in history[k]]
        max_val  = max(all_vals) if all_vals else 1
        if max_val == 0:
            max_val = 1

        for ki, key in enumerate(SERIES_KEYS):
            pts = history[key]
            if len(pts) < 2:
                continue
            color = SERIES_COLORS[ki]
            coords = []
            for i, val in enumerate(pts):
                x = int(CHART_W * i / (MAX_PTS - 1))
                y = int(CHART_H - CHART_H * (val / max_val))
                y = max(4, min(CHART_H - 4, y))
                coords.append((x, y))
            for i in range(len(coords) - 1):
                shapes.append(cv.Line(
                    coords[i][0], coords[i][1],
                    coords[i + 1][0], coords[i + 1][1],
                    paint=ft.Paint(color=color, stroke_width=2),
                ))
            lx, ly = coords[-1]
            shapes.append(cv.Circle(lx, ly, 4,
                                     paint=ft.Paint(color=color,
                                                     style=ft.PaintingStyle.FILL)))

        shapes.append(cv.Text(
            f"max: {max_val:.0f}", x=6, y=8,
            style=ft.TextStyle(color=TEXT_MUT, size=10),
        ))
        chart_canvas.shapes = shapes

    # ── Metric card helper ──────────────────────────────────
    def _mk_metric(title, color):
        val_text = ft.Text("0", size=26, color=color,
                           weight=ft.FontWeight.BOLD,
                           font_family="monospace")
        box = ft.Container(
            content=ft.Column([
                lbl(title, size=10, color=TEXT_MUT),
                val_text,
            ], spacing=2, tight=True),
            bgcolor=f"{color}10",
            border=ft.Border.all(1, f"{color}30"),
            border_radius=10, padding=14, expand=True,
        )
        return box, val_text

    m_boxes   = []
    m_texts   = []
    for i, k in enumerate(SERIES_KEYS):
        b, t = _mk_metric(SERIES_LABELS[i], SERIES_COLORS[i])
        m_boxes.append(b)
        m_texts.append(t)

    up_box, up_t   = _mk_metric("Uptime (h)",    ACCENT2)
    bin_box, bin_t = _mk_metric("Bytes IN (MB)",  TEXT_SEC)
    bout_box,bout_t= _mk_metric("Bytes OUT (MB)", TEXT_SEC)
    extra_boxes = [up_box, bin_box, bout_box]
    extra_texts = [up_t, bin_t, bout_t]

    def _update_metrics(m):
        for i, k in enumerate(SERIES_KEYS):
            v = m.get(k, 0)
            history[k].append(v)
            if len(history[k]) > MAX_PTS:
                history[k].pop(0)
            m_texts[i].value = f"{v:.0f}"
        extra_texts[0].value = f"{m.get('uptime', 0)/3600:.1f}"
        extra_texts[1].value = f"{m.get('bytes_in', 0)/1024/1024:.2f}"
        extra_texts[2].value = f"{m.get('bytes_out',0)/1024/1024:.2f}"
        _redraw()

    def snapshot(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        _update_metrics(db.get_performance_metrics())
        notif.show("Métricas actualizadas", SUCCESS)
        e.page.update()

    start_btn = ft.Container(
        content=ft.Row([
            ft.Icon(ft.Icons.PLAY_ARROW, color=SUCCESS, size=18),
            ft.Text("Iniciar Monitor", color=SUCCESS, size=13, weight=ft.FontWeight.W_500),
        ], spacing=8, alignment=ft.MainAxisAlignment.CENTER),
        bgcolor=f"{SUCCESS}22",
        border_radius=8,
        border=ft.Border.all(1, SUCCESS),
        padding=ft.Padding.symmetric(horizontal=16, vertical=10),
        on_click=None,
        ink=True,
    )

    def toggle_monitor(e):
        if running["v"]:
            running["v"] = False
            start_btn.content = ft.Row([
                ft.Icon(ft.Icons.PLAY_ARROW, color=SUCCESS, size=18),
                ft.Text("Iniciar Monitor", color=SUCCESS, size=13, weight=ft.FontWeight.W_500),
            ], spacing=8, alignment=ft.MainAxisAlignment.CENTER)
        else:
            if not db.is_connected:
                notif.show("Primero conecte a MariaDB", WARNING); return
            running["v"] = True
            start_btn.content = ft.Row([
                ft.Icon(ft.Icons.STOP, color=DANGER, size=18),
                ft.Text("Detener Monitor", color=DANGER, size=13, weight=ft.FontWeight.W_500),
            ], spacing=8, alignment=ft.MainAxisAlignment.CENTER)

            def loop():
                while running["v"]:
                    try:
                        _update_metrics(db.get_performance_metrics())
                        page.update()
                    except Exception:
                        pass
                    try:
                        secs = int(interval_f.value or "3")
                    except Exception:
                        secs = 3
                    time.sleep(secs)

            threading.Thread(target=loop, daemon=True).start()
        e.page.update()

    start_btn.on_click = toggle_monitor

    legend = ft.Row([
        ft.Row([ft.Container(width=18, height=3,
                              bgcolor=SERIES_COLORS[i], border_radius=2),
                lbl(SERIES_LABELS[i], color=TEXT_SEC)], spacing=6)
        for i in range(3)
    ], spacing=20)

    return ft.Column([
        section_title(ft.Icons.SHOW_CHART, "MONITOREO DE RENDIMIENTO"),
        ft.Divider(color=BORDER, height=1),
        card(ft.Row([start_btn,
                     accent_btn("Snapshot", snapshot, ft.Icons.CAMERA_ALT),
                     interval_f,
                     lbl("seg entre lecturas", color=TEXT_MUT)],
                    spacing=12)),
        ft.Row(m_boxes, spacing=12),
        ft.Row(extra_boxes, spacing=12),
        card(ft.Column([
            ft.Row([lbl("GRÁFICA EN TIEMPO REAL", size=11, color=ACCENT),
                    legend],
                   alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Container(
                content=ft.Column([chart_canvas],
                                   scroll=ft.ScrollMode.ALWAYS),
                bgcolor=BG_DARK, border_radius=8,
                border=glow_border(BORDER), padding=10,
                height=CHART_H + 20,
                clip_behavior=ft.ClipBehavior.HARD_EDGE,
            ),
        ], spacing=10)),
    ], spacing=14, scroll=ft.ScrollMode.AUTO)


# ─────────────────────────────────────────────
#  TAB 4 – CONSOLA SQL
# ─────────────────────────────────────────────
SNIPPETS = [
    ("Crear usuario",
     "CREATE USER 'nuevo_user'@'localhost' IDENTIFIED BY 'password123';"),
    ("Crear BD",
     "CREATE DATABASE mi_bd CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"),
    ("Otorgar ALL",
     "GRANT ALL PRIVILEGES ON mi_bd.* TO 'usuario'@'localhost';\nFLUSH PRIVILEGES;"),
    ("Crear rol",
     ("CREATE ROLE 'rol_lectura';\n"
      "GRANT SELECT ON *.* TO 'rol_lectura';\n"
      "GRANT 'rol_lectura' TO 'usuario'@'localhost';")),
    ("Revocar",
     "REVOKE ALL PRIVILEGES ON *.* FROM 'usuario'@'localhost';\nFLUSH PRIVILEGES;"),
    ("Eliminar usuario", "DROP USER IF EXISTS 'usuario'@'localhost';"),
    ("Eliminar rol",     "DROP ROLE IF EXISTS 'rol_lectura';"),
    ("Listar usuarios",
     "SELECT User, Host, plugin FROM mysql.user ORDER BY User;"),
    ("Ver grants",       "SHOW GRANTS FOR 'usuario'@'localhost';"),
    ("Ver procesos",     "SHOW FULL PROCESSLIST;"),
    ("Variables globales","SHOW GLOBAL VARIABLES LIKE '%max%';"),
    ("Estado global",    "SHOW GLOBAL STATUS LIKE '%connect%';"),
]


def build_sql_tab(notif: Notif):
    sql_editor = ft.TextField(
        label="Editor SQL",
        multiline=True, min_lines=10, max_lines=16,
        hint_text="-- Escriba sentencias SQL aquí\nSELECT * FROM mysql.user LIMIT 5;",
        bgcolor=BG_DARK,
        border_color=BORDER,
        focused_border_color=ACCENT,
        label_style=ft.TextStyle(color=TEXT_SEC, size=12),
        text_style=ft.TextStyle(color=ACCENT, size=13, font_family="monospace"),
        hint_style=ft.TextStyle(color=TEXT_MUT, size=12, font_family="monospace"),
        border_radius=8,
        cursor_color=ACCENT,
        expand=True,
    )
    result_tbl = ft.DataTable(
        columns=[ft.DataColumn(ft.Text("—", color=TEXT_MUT))],
        rows=[],
        border=ft.Border.all(1, BORDER),
        border_radius=8,
        heading_row_color=f"{ACCENT}15",
        data_row_color={"hovered": f"{ACCENT}08"},
        column_spacing=20,
    )
    msg_text  = ft.Text("", color=TEXT_SEC, size=12,
                         font_family="monospace", selectable=True)
    time_text = ft.Text("", size=11, color=TEXT_MUT, font_family="monospace")

    def insert_snippet(sql_text):
        def _h(e):
            sql_editor.value = sql_text
            e.page.update()
        return _h

    snippet_btns = ft.Row(
        controls=[
            ft.TextButton(
                s[0], on_click=insert_snippet(s[1]),
                style=ft.ButtonStyle(
                    color={"": TEXT_SEC, "hovered": ACCENT},
                    padding=ft.Padding.symmetric(horizontal=8, vertical=4),
                ))
            for s in SNIPPETS
        ],
        wrap=True, spacing=4, run_spacing=4,
    )

    def run_sql(e):
        if not db.is_connected:
            notif.show("Primero conecte a MariaDB", WARNING); return
        sql = sql_editor.value.strip()
        if not sql:
            notif.show("Escriba una sentencia SQL", WARNING); return
        t0      = time.time()
        results = db.execute_script(sql)
        elapsed = (time.time() - t0) * 1000
        time_text.value = f"{elapsed:.1f} ms"

        msgs      = []
        last_data = None
        all_ok    = True
        for stmt, ok, msg, data in results:
            icon = "✓" if ok else "✗"
            msgs.append(f"{icon} {stmt[:70]}  →  {msg}")
            if not ok:
                all_ok = False
            if data:
                last_data = data

        msg_text.value = "\n".join(msgs)
        msg_text.color = SUCCESS if all_ok else DANGER

        if last_data:
            cols, rows = last_data
            result_tbl.columns = [
                ft.DataColumn(ft.Text(c, color=ACCENT, size=12,
                                       font_family="monospace"))
                for c in cols]
            result_tbl.rows = [
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(
                        str(v) if v is not None else "NULL",
                        color=TEXT_PRI, size=11, font_family="monospace"))
                    for v in row])
                for row in rows[:500]]
        else:
            result_tbl.columns = [ft.DataColumn(ft.Text("—", color=TEXT_MUT))]
            result_tbl.rows    = []

        e.page.update()

    def clear_all(e):
        sql_editor.value    = ""
        msg_text.value      = ""
        time_text.value     = ""
        result_tbl.columns  = [ft.DataColumn(ft.Text("—", color=TEXT_MUT))]
        result_tbl.rows     = []
        e.page.update()

    return ft.Column([
        section_title(ft.Icons.TERMINAL, "CONSOLA SQL"),
        ft.Divider(color=BORDER, height=1),
        card(ft.Column([
            lbl("SNIPPETS RÁPIDOS — Usuarios & Roles", size=11, color=ACCENT),
            sp(4),
            snippet_btns,
        ], spacing=8)),
        card(ft.Column([
            ft.Row([
                lbl("EDITOR SQL", size=11, color=ACCENT),
                ft.Row([
                    time_text,
                    accent_btn("▶  Ejecutar", run_sql, ft.Icons.PLAY_ARROW),
                    ft.TextButton("Limpiar", on_click=clear_all,
                                   style=ft.ButtonStyle(
                                       color={"": TEXT_MUT})),
                ], spacing=8),
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            sql_editor,
        ], spacing=8)),
        card(ft.Column([
            lbl("RESULTADO", size=11, color=ACCENT),
            msg_text,
            ft.Container(
                content=ft.Column(
                    [ft.Row([result_tbl], scroll=ft.ScrollMode.ALWAYS)],
                    scroll=ft.ScrollMode.AUTO),
                bgcolor=BG_DARK, border_radius=8,
                border=glow_border(BORDER), padding=10, height=300,
            ),
        ], spacing=8)),
    ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)


# ─────────────────────────────────────────────
#  SIDEBAR NAV ITEM
# ─────────────────────────────────────────────
NAV_ITEMS = [
    (ft.Icons.POWER_SETTINGS_NEW, "Conexión"),
    (ft.Icons.SAVE,               "Backup & Restore"),
    (ft.Icons.IMPORT_EXPORT,      "CSV I/O"),
    (ft.Icons.SHOW_CHART,         "Rendimiento"),
    (ft.Icons.TERMINAL,           "Consola SQL"),
]


def nav_item(icon, label_text, index, selected, on_select):
    sel = index == selected
    return ft.Container(
        content=ft.Row([
            ft.Icon(icon, color=ACCENT if sel else TEXT_MUT, size=20),
            ft.Text(label_text,
                    color=ACCENT if sel else TEXT_MUT, size=13,
                    weight=ft.FontWeight.BOLD if sel else ft.FontWeight.NORMAL,
                    font_family="monospace"),
        ], spacing=12),
        bgcolor=f"{ACCENT}15" if sel else "transparent",
        border_radius=8,
        border=ft.Border.all(1, ACCENT if sel else "transparent"),
        padding=ft.Padding.symmetric(horizontal=16, vertical=12),
        on_click=lambda e: on_select(index),
        ink=True,
    )


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main(page: ft.Page):
    page.title   = "MariaDB Admin Studio"
    page.bgcolor = BG_DARK
    page.padding = 0
    page.window.width      = 1300
    page.window.height     = 840
    page.window.min_width  = 960
    page.window.min_height = 620

    notif = Notif(page)

    content_area = ft.Ref[ft.Container]()
    sidebar_col  = ft.Ref[ft.Column]()
    dot_ref      = ft.Ref[ft.Container]()
    lbl_ref      = ft.Ref[ft.Text]()

    _cache: dict = {}

    def get_tab(idx):
        if idx not in _cache:
            builders = {
                0: lambda: build_connection_tab(notif, on_connected),
                1: lambda: build_backup_tab(notif),
                2: lambda: build_csv_tab(notif),
                3: lambda: build_perf_tab(notif, page),
                4: lambda: build_sql_tab(notif),
            }
            _cache[idx] = builders[idx]()
        return _cache[idx]

    def build_nav(selected):
        return [
            ft.Container(
                content=ft.Row([
                    ft.Container(
                        content=ft.Icon(ft.Icons.STORAGE, color=BG_DARK, size=18),
                        bgcolor=ACCENT, border_radius=8, padding=8,
                    ),
                    ft.Column([
                        ft.Text("MariaDB", size=14, color=TEXT_PRI,
                                weight=ft.FontWeight.BOLD,
                                font_family="monospace"),
                        ft.Text("Admin Studio", size=10, color=TEXT_MUT,
                                font_family="monospace"),
                    ], spacing=0, tight=True),
                ], spacing=10),
                padding=ft.Padding.symmetric(horizontal=16, vertical=20),
            ),
            ft.Divider(color=BORDER, height=1),
            sp(6),
            *[nav_item(NAV_ITEMS[i][0], NAV_ITEMS[i][1],
                       i, selected, switch_tab)
              for i in range(len(NAV_ITEMS))],
            ft.Divider(color=BORDER, height=1),
            sp(6),
            ft.Container(
                content=ft.Column([
                    lbl("SERVIDOR", size=10, color=TEXT_MUT),
                    ft.Row([
                        ft.Container(ref=dot_ref, width=8, height=8,
                                     bgcolor=DANGER, border_radius=4),
                        ft.Text(ref=lbl_ref, value="Sin conexión",
                                size=11, color=TEXT_MUT,
                                font_family="monospace"),
                    ], spacing=6),
                ], spacing=6),
                padding=ft.Padding.symmetric(horizontal=16, vertical=12),
            ),
        ]

    def switch_tab(idx):
        sidebar_col.current.controls = build_nav(idx)
        content_area.current.content = ft.Container(
            content=get_tab(idx),
            expand=True, padding=24,
        )
        page.update()

    def on_connected(state: bool):
        if dot_ref.current:
            dot_ref.current.bgcolor = SUCCESS if state else DANGER
        if lbl_ref.current:
            lbl_ref.current.value = (
                f"{db.config.get('user','?')}@{db.config.get('host','?')}"
                if state else "Sin conexión")
            lbl_ref.current.color = SUCCESS if state else TEXT_MUT

    page.add(ft.Row([
        # ── sidebar ──────────────────────────────
        ft.Container(
            content=ft.Column(
                ref=sidebar_col,
                controls=build_nav(0),
                scroll=ft.ScrollMode.AUTO,
                spacing=4,
            ),
            width=220,
            bgcolor=BG_CARD,
            border=ft.Border(right=ft.BorderSide(1, BORDER)),
        ),
        # ── main area ────────────────────────────
        ft.Container(
            expand=True,
            content=ft.Column([
                # top bar
                ft.Container(
                    content=ft.Row([
                        ft.Text("MariaDB Admin Studio", size=13,
                                color=TEXT_MUT, font_family="monospace"),
                        ft.Text(datetime.now().strftime("%a %d %b %Y"),
                                size=12, color=TEXT_MUT,
                                font_family="monospace"),
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    bgcolor=BG_CARD,
                    border=ft.Border(
                        bottom=ft.BorderSide(1, BORDER)),
                    padding=ft.Padding.symmetric(horizontal=24, vertical=12),
                ),
                # content pane
                ft.Container(
                    ref=content_area,
                    expand=True,
                    content=ft.Container(
                        content=get_tab(0),
                        expand=True, padding=24,
                    ),
                ),
            ], spacing=0, expand=True),
        ),
    ], spacing=0, expand=True))


ft.app(target=main)