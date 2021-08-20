from airflow.plugins_manager import AirflowPlugin 
import repackage
repackage.up()
from operators.twitter_operator import TwitterOperator

class AluraAirflowPlugin(AirflowPlugin) :
    name = "Alura"
    operators = [TwitterOperator]