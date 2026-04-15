# MariaDB Admin Studio 🗄️

Aplicación de escritorio moderna para administrar MariaDB, construida con Python + Flet.

## Características

| Módulo | Funcionalidades |
|--------|----------------|
| 🔌 **Conexión** | Conectar/desconectar, selección de BD, estado en tiempo real |
| 💾 **Backup & Restore** | `mysqldump` o exportación alternativa vía connector; restauración desde .sql |
| 📊 **CSV I/O** | Exportar cualquier tabla a CSV, importar CSV a tabla existente, vista previa de datos |
| 📈 **Rendimiento** | Monitoreo en tiempo real (queries, conexiones, slow queries, uptime, bytes), gráfica dinámica |
| 💻 **Consola SQL** | Editor con snippets para usuarios/roles, ejecución multi-sentencia, tabla de resultados |

## Instalación

```bash
# 1. Clonar o copiar los archivos
cd mariadb_admin

# 2. Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Ejecutar
python main.py
```

## Requisitos del sistema

- Python 3.9+
- MariaDB / MySQL corriendo localmente o en red
- `mysqldump` en el PATH (para backup nativo; hay fallback automático si no está)

## Snippets SQL incluidos

La **Consola SQL** incluye accesos rápidos para:
- `CREATE USER` / `DROP USER`
- `CREATE ROLE` / `DROP ROLE` (MariaDB 10.0.5+)
- `GRANT` / `REVOKE` privilegios
- `SHOW GRANTS` / `SHOW FULL PROCESSLIST`
- `SHOW GLOBAL VARIABLES`

## Estructura de archivos

```
mariadb_admin/
├── main.py          # Aplicación completa (single-file)
├── requirements.txt # Dependencias Python
└── README.md        # Este archivo
```

## Notas

- El **backup alternativo** (cuando `mysqldump` no está en PATH) genera un archivo SQL usando el conector Python directamente.
- La **gráfica de rendimiento** requiere permisos para ejecutar `SHOW GLOBAL STATUS` (normalmente requiere usuario con privilegios `SUPER` o `REPLICATION CLIENT`).
- El **intervalo de monitoreo** es configurable desde la UI (default: 3 segundos).
