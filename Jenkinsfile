pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('sbt')}:$PATH"
    }
    stages {
        stage('Sanity-check') {
            steps {
                sh 'echo $PATH'
                sh 'which gcloud'
            }
        }
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
            // when { branch 'master' }
            environment {
                PATH = "${tool('gcloud')}:${tool('jq')}:$PATH"
            }
            steps {
                script {
                    def saVaultKey = 'secret/dsde/secret/dsde/monster/dev/gcr/broad-dsp-gcr-public-sa.json'
                    def saTmp = '${WORKSPACE}/sa-key.json'
                    def dockerProjects = [
                            'aws-to-gcp-agent',
                            'aws-to-gcp-agent-deploy',
                            'echo-agent',
                            'echo-agent-deploy',
                            'manager',
                            'manager-deploy',
                            'manager-migrations'
                    ]

                    def steps = [
                            '#!/bin/bash',
                            'set +x',
                            'echo Publishing artifacts...',
                            'export CLOUDSDK_CONFIG=${WORKSPACE}',
                            "vault read -format=json $saVaultKey | jq .data > $saTmp",
                            "gcloud auth activate-service-account \$(vault read -field=client_email $saVaultKey) --key-file=$saTmp",
                            'gcloud auth configure-docker --quiet',
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
