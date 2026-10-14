pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        PIP_DISABLE_PIP_VERSION_CHECK = '1'
        PYTHONDONTWRITEBYTECODE = '1'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Tool versions') {
            steps {
                sh 'python3 --version'
                sh 'git --version'
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''
                    python3 -m venv .venv-ci
                    .venv-ci/bin/python -m pip install -r requirements-dev.txt
                '''
            }
        }

        stage('Python syntax') {
            steps {
                sh '''
                    .venv-ci/bin/python -m py_compile \
                        producer.py \
                        consumer.py \
                        database_writer.py \
                        dashboard/app.py
                '''
            }
        }

        stage('Unit tests') {
            steps {
                sh '''
                    mkdir -p reports
                    .venv-ci/bin/python -m pytest -v \
                        --junitxml=reports/pytest.xml
                '''
            }
        }
    }

    post {
        always {
            junit testResults: 'reports/pytest.xml',
                  allowEmptyResults: true
            archiveArtifacts artifacts: 'reports/pytest.xml',
                             allowEmptyArchive: true
        }
    }
}
