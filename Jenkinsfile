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
                // If we have a change_id then this is a pull request, otherwise it's a change to the main
                // repo and the image should be stored in docker
                script {
                    entityd_tag = "${MASTER_DOCKER_REGISTRY}/entityd:latest".toLowerCase()
                    entityd_test_tag = "${MASTER_DOCKER_REGISTRY}/entityd-test:latest".toLowerCase()
                    kubectl_tag = "${MASTER_DOCKER_REGISTRY}/kubectl-entityd:latest".toLowerCase()
                }

                node('docker') {
                    // Ensure the node has the latest code
                    cleanWs()
                    checkout scm
                    script {
                        build_key = 'building'
                        build_desc = 'Building docker images'
                        bitbucketStatusNotify(buildState: 'INPROGRESS',
                            buildKey: build_key,
                            buildName: build_key,
                            buildDescription: build_desc)

                        try {
                            entityd_image = docker.build(entityd_tag, '-f entityd.Dockerfile .')
                            entityd_image_id = entityd_image.id

                            entityd_test_image = docker.build(entityd_test_tag, "-f entityd-test.Dockerfile .")
                            entityd_test_image_id = entityd_test_image.id

                            withCredentials([string(credentialsId: 'google-cobesaas', variable: 'JSON_FILE')]) {
                                sh """
                                    docker login -u _json_key -p '${JSON_FILE}' https://eu.gcr.io
                                """
                                kubectl_image = docker.build(kubectl_tag, '-f kubectl-entityd.Dockerfile .')
                                kubectl_image_id = kubectl_image.id

                                sh "docker logout https://eu.gcr.io"
                            }

                            // Push our built images to the jenkins master registry
                            entityd_image.push()
                            entityd_test_image.push()
                            kubectl_image.push()
                        } catch (error) {
                            bitbucketStatusNotify(buildState: 'FAILED',
                                buildKey: build_key,
                                buildName: build_key,
                                buildDescription: build_desc)
                            echo "Caught: ${error}"
                            throw error
                        }

                        bitbucketStatusNotify(buildState: 'SUCCESSFUL',
                            buildKey: build_key,
                            buildName: build_key,
                            buildDescription: build_desc)
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
                    entityd_image = docker.image(entityd_image_id)
                    entityd_image.pull()
                    kubectl_image = docker.image(kubectl_image_id)
                    kubectl_image.pull()

                    sh "docker tag ${entityd_image.id} eu.gcr.io/cobesaas/entityd:latest"
                    sh "docker tag ${kubectl_image.id} eu.gcr.io/cobesaas/kubectl-entityd:latest"

                    withCredentials([string(credentialsId: 'google-cobesaas', variable: 'JSON_FILE')]) {
                        sh """
                            docker login -u _json_key -p '${JSON_FILE}' https://eu.gcr.io
                            docker push eu.gcr.io/cobesaas/entityd:latest
                            docker push eu.gcr.io/cobesaas/kubectl-entityd:latest
                            docker logout https://eu.gcr.io
                        """
                    }
                }
            }
        }
    }
}
