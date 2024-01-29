# filename_plugin_operator.py
from airflow.plugins_manager import AirflowPlugin
from filename_plugin import get_file_name

class FilenamePluginOperator(AirflowPlugin):
    name = "filename_plugin_operator"
    operators = []

    # Optional class variables to expose to Airflow UI
    flask_blueprints = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    menu_links = []

    # Expose the get_file_name function as a macro
    @classmethod
    def get_additional_macro(cls):
        return {
            "get_file_name": get_file_name
        }
