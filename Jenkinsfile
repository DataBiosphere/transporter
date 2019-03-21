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
                sh 'sbt -client scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt -client Compile/compile Test/compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt -client test'
            }
        }
    }
    post {
        always {
            junit '**/target/test-reports/*'
        }
        cleanup {
            sh 'sbt -client shutdown'
            cleanWs()
        }
    }
}
