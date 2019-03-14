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
        stage('Compile') {
            steps {
                sh 'sbt compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt test'
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
