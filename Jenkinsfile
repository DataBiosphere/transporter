pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('gcloud')}:${tool('sbt')}:$PATH"
    }
    stages {
        stage('Sanity-check') {
            steps {
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
            steps {
                script {
                    def dockerProjects = [
                    'aws-to-gcp-agent',
                    'aws-to-gcp-agent-deploy',
                    'echo-agent',
                    'echo-agent-deploy',
                    'manager',
                    'manager-deploy',
                    'manager-migrations']
                    sh "sbt ${dockerProjects.collect { "transporter-$it/Docker/publish" }.join(' ') }"
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
