@Library('shared-library') _

def credentialsId = 'jenkins-rarible-ci'

pipeline {
    agent any

    options {
        disableConcurrentBuilds()
    }

    stages {
        stage('test') {
            steps {
                sh 'mvn clean test'
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }
        stage('deploy') {
            when {
                branch 'master'
            }
            steps {
                deployToMaven(credentialsId)
            }
        }
    }
}