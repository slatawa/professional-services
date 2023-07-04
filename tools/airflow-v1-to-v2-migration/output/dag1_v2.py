from airflow import DAG
# Migration Utility Generated Comment -- Change Type = Changes in import , Impact = Import Statement Changed
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateInlineWorkflowTemplateOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('dataproc_workflow_dag', default_args=default_args, schedule_interval=None) as dag:

# Migration Utility Generated Comment -- Change Type = Changes in Operator , Impact = Operator Name Change
    instantiate_workflow = DataprocInstantiateInlineWorkflowTemplateOperator(
        task_id='instantiate_workflow',
        project_id='your_project_id',
        template={
            'placement': {
                'managedCluster': {
                    'clusterName': 'your_cluster_name',
                    'config': {
                        'gceClusterConfig': {
                            'zoneUri': 'your_zone_uri'
                        },
                        'masterConfig': {
                            'numInstances': 1,
                            'machineTypeUri': 'n1-standard-2'
                        },
                        'workerConfig': {
                            'numInstances': 2,
                            'machineTypeUri': 'n1-standard-2'
                        }
                    }
                }
            }
        }
    )

    instantiate_workflow
