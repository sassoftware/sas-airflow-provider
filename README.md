# SAS&reg; Airflow Provider

## Current major capabilities of the SAS&reg; Studio Flow Operator

* Execute a SAS Studio Flow stored either on the File System or in SAS Content
* Select the Compute Context to be used for execution of a SAS Studio Flow
* Specify whether SAS logs of a SAS Studio Flow execution should be returned and displayed in Airflow
* Specify parameters (init_code, wrap_code) to be used for code generation
* Honor return code of a SAS Studio Flow in Airflow. In particular, if a SAS Studio Flow fails, Airflow raises an exception as well and stops execution
* Authenticate via oauth token or via user/password (i.e. generation of oauth token prior to each call)


## Getting started
Please note that this file is no substitute for reading and understanding the Airflow documentation. This file is only intended to provide a quick start for the SAS providers. Unless an issue relates specifically to the SAS providers, the Airflow documentation should be consulted.
### Install Airflow
Follow instructions at https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html to install Airflow.
If you just want to evaluate the SAS providers, then the simplest path would be to install via PYPI and run Airflow on the local machine in a virtual environment. 

### Install the SAS provider
If you want to build the package from these sources, install the build module using `pip install build` and then run `python -m build` from the root of the repository which will create a wheel file in the dist subdirectory. 

#### Installing in a local virtual environment
The SAS provider is available as a package published in PyPI. To install it, switch to the Python environment where Airflow is installed, and run the following command:

`pip install sas-airflow-provider`

If you would like to install the provider from a package you built locally, run:

`pip install dist/sas_airflow_provider_xxxxx.whl`

#### Installing in a container
There are a few ways to provide the package:
- Environment variable: ```_PIP_ADDITIONAL_REQUIREMENTS``` Set this variable to the command line that will be passed to ```pip install```
- Create a dockerfile that adds the pip install command to the base image and edit the docker-compose file to use "build" (there is a comment in the docker compose file where you can change it)

### Create a connection to SAS
In order to connect to SAS Viya from the Airflow operator, you will need to create a connection. The easiest way to do this is to go into the Airflow UI under Admin/Connections and create a new connection using the blue + button. Select SAS from the list of connection types, and enter sas_default as the name. The applicable fields are host (http or https url to your SAS Viya install), login and password. It is also possible to specify an OAuth token by creating a json body in the extra field. For example `{"token": "oauth_token_here"}`. If a token is found it is used instead of the user/password.
Please be aware of security considerations when storing sensitive information in a
connection. Consult https://airflow.apache.org/docs/apache-airflow/stable/security/index.html for details.
TLS verification can be disabled (not recommended) by specifying the following in
the extra field `{"ssl_certificate_verification": false }`. 

In addition, a custom TLS CA certificate bundle file can be used as follows:
`{"ssl_certificate_verification": "/path/to/trustedcerts.pem"}`. Note that the path used for the CA certificate bundle must reference a location within the Airflow pods.

Inbound security rules must allow communication from the Airflow web server pod through the ingress defined for SAS Viya. Connection timeout errors might occur if the rule is not in place.

### Running a DAG with a SAS provider
See example files in the src/sas_airflow_provider/example_dags directory. These dags can be modified and 
placed in your Airflow dags directory. 

Mac note: If you are running Airflow standalone on a Mac, there is a known issue regarding how process forking works.
This causes issues with the urllib which is used by the operator. To get around it set NO_PROXY=* in your environment
prior to running Airflow in standalone mode.
Eg:
`export NO_PROXY="*"`

### Prerequisites for running demo DAGs
You will need to create a SAS Studio Flow or a Job Definition before you can reference it from a DAG. The easiest way is to use the SAS Studio UI to do this.


## Contributing
We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for
details on how to submit contributions to this project.

## License
This project is licensed under the [Apache 2.0 License](LICENSE).
