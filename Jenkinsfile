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

            def dockerProjects = ['transporter-manager', 'transporter-echo-agent', 'transporter-aws-to-gcp-agent']

            steps {
                sh 'sbt all ' + dockerProjects.collect { "$it/Docker/publish" }.join(' ')
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
