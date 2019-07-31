pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
    }
    environment {
        PATH = "${tool('sbt')}:$PATH"
    }
    stages {
        stage('Check formatting') {
            steps {
                sh 'sbt scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt Compile/compile Test/compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt test'
            }
        }
        stage('Publish') {
            when { branch 'master' }
            environment {
                PATH = "${tool('gcloud')}:${tool('vault')}:${tool('jq')}:$PATH"
                // Some wiring is broken between the custom-tools plugin and
                // the pipeline plugin which prevents these vars from being
                // injected when pulling in the custom 'vault' tool.
                VAULT_ADDR = 'https://clotho.broadinstitute.org:8200'
                VAULT_TOKEN_PATH = '/etc/vault-token-monster'
            }
            steps {
                script {
                    def saVaultKey = 'secret/dsde/monster/dev/gcr/broad-dsp-gcr-public-sa.json'
                    def saTmp = '${WORKSPACE}/sa-key.json'
                    def dockerProjects = [
                            'aws-to-gcp-agent',
                            'aws-to-gcp-agent-deploy',
                            'echo-agent',
                            'manager',
                            'manager-deploy',
                            'manager-migrations'
                    ]

                    def steps = [
                            '#!/bin/bash',
                            'set +x -euo pipefail',
                            'echo Publishing artifacts...',
                            // Pull the login key for the service account that can publish to GCR.
                            'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH)',
                            "vault read -format=json $saVaultKey | jq .data > $saTmp",
                            // Jenkins' home directory is read-only, so we have to set up
                            // local storage to write temporary gcloud / docker configs.
                            'export CLOUDSDK_CONFIG=${WORKSPACE}/.gcloud',
                            'export DOCKER_CONFIG=${WORKSPACE}/.docker',
                            'mkdir -p ${CLOUDSDK_CONFIG} ${DOCKER_CONFIG}',
                            'cp -r ${HOME}/.docker/* ${DOCKER_CONFIG}/',
                            // Log into gcloud, then link Docker to gcloud.
                            "gcloud auth activate-service-account \$(vault read -field=client_email $saVaultKey) --key-file=$saTmp",
                            'gcloud auth configure-docker --quiet',
                            // Push :allthethings: if the setup succeeded.
                            "sbt ${dockerProjects.collect { "transporter-$it/Docker/publish" }.join(' ') }"
                    ]

                    sh steps.join('\n')
                }
            }
        }
    }
    post {
        always {
            junit '**/target/test-reports/*'
        }
        cleanup {
            cleanWs()
        }
    }
}
