pipeline {
    agent any

    stages {
        stage("Perform Action") {
            steps {
                // sh 'python3 -m pip install --upgrade pip'
                // sh 'python3 -m pip install -r requirements.txt'
                sh 'python3 notifications_load.py'
            }
        }
    }
}