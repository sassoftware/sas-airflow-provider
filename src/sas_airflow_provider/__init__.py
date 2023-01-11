def get_provider_info():
    return {
        "package-name": "sas-airflow-provider",
        "name": "SAS Airflow Provider",
        "description": "Allows execution of SAS Studio Flows and Jobs",
        "connection-types": [
            {"hook-class-name": "sas_airflow_provider.hooks.sas.SasHook",
             "connection-type": "sas"}
        ],
        "versions": ["0.0.1"]
    }
