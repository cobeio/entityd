#!/usr/bin/env groovy
@Library('cobe') _

runPipeline name: "entityd",
    timeoutMins: 30,
    images: [
        [name: "entityd"],
        [name: "entityd-test"],
        [name: "kubectl-entityd"],
    ],
    tests: [
        [
            name:"Unit Tests",
            key:"py.test",
            image:"entityd-test",
            cmd: "jenkins-pytest",
            timeoutMins: 5,
            retries: 3,
            resultsFolder: "/opt/cobe-agent/src/results"
        ],
        [
            name:"Linting Tests",
            key:"pylint",
            image:"entityd-test",
            cmd: "pylint",
            timeoutMins: 5,
            resultsFolder: "/opt/cobe-agent/src/results"]
    ],
    publishImages: ["entityd", "kubectl-entityd"]
