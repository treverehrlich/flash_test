import mlflow

def move_mlflow_model(source, target, client, reg_model_name, archive_existing_versions):
    """
    Move the most recent version of a registered model.
    This method will raise an error if there is
    no such registered model name
    
    :param source: Where to move from
    :type source: str

    :param target: Where to move to
    :type target: str

    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str

    :param archive_existing_versions: Whether or not to archive the
        previous staged model
    :type archive_existing_versions: bool
    """
    model_version = client.get_latest_versions(reg_model_name, stages=[source])
    client.transition_model_version_stage(
        name=reg_model_name,
        version=model_version[0].version,
        stage=target,
        archive_existing_versions=archive_existing_versions
    )


def stage_mlflow_model(client, reg_model_name, archive_existing_versions=True):
    """
    Move the most recent version of a
    registered model from "None" to "Staging".
    This method will raise an error if there is
    no such registered model name
    
    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str

    :param archive_existing_versions: Whether or not to archive the
        previous staged model
    :type archive_existing_versions: bool
    """
    move_mlflow_model(
        "None",
        "Staging",
        client,
        reg_model_name,
        archive_existing_versions)


def prod_model_exists(client, reg_model_name):
    """
    Check if the registered model name has a production version.
    
    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str

    :return: Boolean
    :rtype: bool
    """
    return bool(client.get_latest_versions(reg_model_name, stages=["Production"]))


def move_stage_to_prod(client, reg_model_name, archive_existing_versions=True):
    """
    Move the most recent version of a
    registered model from "Staging" to "Production".
    This method will raise an error if there is
    no such registered model name in "Staging"
    
    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str

    :param archive_existing_versions: Whether or not to archive the
        previous staged model
    :type archive_existing_versions: bool
    """
    move_mlflow_model(
        "Staging",
        "Production",
        client,
        reg_model_name,
        archive_existing_versions)

def rollback_stage(client, reg_model_name):
    """
    Move the most recent version of a
    registered model from "Staging" to "None".
    This method will raise an error if there is
    no such registered model name in "Staging"
    
    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str
    """
    move_mlflow_model(
        "Staging",
        "None",
        client,
        reg_model_name,
        archive_existing_versions=False)

def remove_prod_model(client, reg_model_name):
    """
    Move the most recent version of a
    registered model from "Staging" to "None".
    This method will raise an error if there is
    no such registered model name in "Staging"
    
    :param client: The mlflow client
    :type client: MlflowClient()

    :param reg_model_name: The registered model name
    :type reg_model_name: str
    """
    move_mlflow_model(
        "Production",
        "None",
        client,
        reg_model_name,
        archive_existing_versions=False)
