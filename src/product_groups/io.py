from jinja2 import Template
from pathlib import Path
from product_groups.logger import setup_logger

logger = setup_logger(__name__)


def render_sql(name: str, folder: str = "sql", extension: str = ".sql", **kwargs) -> str:
    """
    Loads an SQL template by name (without a full path) and renders it with provided parameters.

    Parameters
    ----------
    name : str
        Name of the SQL file without extension (e.g., 'get_receipts')
    folder : str
        Directory where SQL templates are stored (default: 'sql')
    extension : str
        File extension of SQL templates (default: '.sql')
    kwargs : dict
        Template parameters to be rendered

    Returns
    -------
    str
        Final SQL query with parameters applied
    """
    path = Path(folder) / f"{name}{extension}"
    if not path.exists():
        logger.error(f"SQL file not found: {path}")
        raise FileNotFoundError(f"SQL file not found: {path}")

    template = Template(path.read_text(encoding="utf-8"))
    rendered = template.render(**kwargs)

    logger.info(f"Rendered SQL: {path.name}")
    logger.debug(f"SQL content:\n{rendered}")

    return rendered
