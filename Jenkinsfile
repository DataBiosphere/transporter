pipeline {
    agent any
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
    }
    post {
        cleanup {
            cleanWs()
        }
    }
}
