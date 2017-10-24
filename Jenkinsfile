#!/usr/bin/env groovy
@Library('cobe') _

pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        lock('entityd-docker-images')
        timeout(30)
    }

    environment {
        entityd_image_id = ""
        entityd_test_image_id = ""
        kubectl_image_id = ""
    }

    stages {
        stage("Build") {
            steps{
                node('docker') {
                    script {
                        rerunBuildOne(env.BUILD_ID, env.JOB_NAME)
                    }
                    cleanWs()

                    script {
                        // Ensure the node has the latest code
                        cobeCheckout()
                        entityd_image_id = dockerBuild('entityd')
                        entityd_test_image_id = dockerBuild('entityd-test')
                        kubectl_image_id = dockerBuild('kubectl-entityd')
                    }
                }
            }
        }
        stage("Test"){
            steps{
                parallel (
                    "Unit Tests": {
                        node('docker'){
                            cleanWs()

                            script {
                                sh "docker pull ${entityd_test_image_id}"
                                // Try tests 3 times as entityd tests can hang and timeout
                                runInvoke(entityd_test_image_id, "py.test",
                                    'Running unit tests', '',
                                    'jenkins-pytest', 5, true, 3){ String container_id ->
                                        sh "docker cp ${container_id}:/entityd/results results"
                                        stash includes: 'results/**', name: 'unit-tests'
                                }
                            }
                        }
                    },

                    "Linting Tests": {
                        node('docker'){
                            cleanWs()

                            script {
                                sh "docker pull ${entityd_test_image_id}"
                                runInvoke(entityd_test_image_id, 'pylint',
                                'Running linting tests', '',
                                'pylint', 5, true) { String container_id ->
                                    sh "docker cp ${container_id}:/entityd/results results"
                                    stash includes: 'results/**', name: 'lint-tests'
                                }
                            }
                        }
                    }
                )
            }
        }

        stage('Publish Image') {
            // If the change id is null, this is a change to the main branch so we need
            // to push the docker image to google
            when {
                expression { env.CHANGE_ID == null }
            }
            steps{
                script {
                    publishImage(entityd_image_id, "entityd", "latest")
                    publishImage(kubectl_image_id, "kubectl-entityd", "latest")
                }
            }
        }
    }
    post {
        always {
            script {
                unstash 'unit-tests'
                unstash 'lint-tests'
                collectResults()
            }
        }
    }
}
