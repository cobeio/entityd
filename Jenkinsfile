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
                    // Ensure the node has the latest code
                    cleanWs()
                    checkout scm
                    script {
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
                            // Ensure the node has the latest code
                            cleanWs()
                            checkout changelog:false, scm: scm

                            script {
                                sh "docker pull ${entityd_test_image_id}"
                                runInvoke(entityd_test_image_id, "py.test",
                                    'Running unit tests', '',
                                    'jenkins-pytest', 5, true){ String container_id ->
                                        sh "docker cp ${container_id}:/entityd/results results"
                                        if (fileExists('results/test_results.xml')) {
                                            junit "results/test_results.xml"
                                            step([$class: 'CoberturaPublisher', coberturaReportFile: 'results/coverage.xml'])
                                        }
                                    }
                            }
                        }
                    },

                    "Linting Tests": {
                        node('docker'){
                            // Ensure the node has the latest code
                            cleanWs()
                            checkout changelog:false, scm: scm

                            script {
                                sh "docker pull ${entityd_test_image_id}"
                                runTestSteps(entityd_test_image_id, "pylint", 'Running linting tests', ''){
                                        pylint = sh(script:'/opt/entityd/bin/invoke pylint', returnStatus: true)
                                        warnings parserConfigurations: [[parserName: 'PyLint', pattern: 'results/pylint.log']]
                                        return pylint
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
}
