

def deploymentStatus = [:]
def deploymentStatusList = []

pipeline {
    agent any  
    environment {
        registry = "dev.exactspace.co"
        repo_name = "coating-score"
        service_type = "api"
        VERSION = "${env.BUILD_ID}"
        APP_NAME = "${repo_name}-es"
        BRANCH_NAME = "${scm.branches[0].name}"
    }
    stages {
        stage('Get Branch Name') {
            steps {
                script {
                    echo "Current Branch : ${BRANCH_NAME}"
                }
            }
        }
// MAIN BRANCH
        /*stage("Tag old image as r0") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh """
                sudo docker pull $registry/$APP_NAME:r1
                sudo docker tag $registry/$APP_NAME:r1 $registry/$APP_NAME:r0
                sudo docker push $registry/$APP_NAME:r0
                """
            }
        }*/
        stage("get scm for r1"){
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps{
                checkout scmGit(branches: [[name: '*/main']], extensions: [[$class: 'SubmoduleOption', recursive: true]], userRemoteConfigs: [[url: "git@github.com:exact-space/${repo_name}.git"]])
                
            }
        }
        stage("cython compilation for r1 ") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("gcc for r1") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "gcc index.c -I/usr/include/python3.10/ -Wall -Wextra -O2 -g -o index -lpython3.10"
            }
        }
        stage("building r1 images") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:r1 ."
            }
        }
        stage("tagging images-r1") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "sudo docker tag $APP_NAME:r1 $registry/$APP_NAME:r1"
            }
        }
        stage("remove old docker image-r1") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "sudo docker image rm $APP_NAME:r1"
            }
        }
        stage("image push-r1") {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                sh "sudo docker push $registry/$APP_NAME:r1"
            }
        }
        /*stage('deploying latestimage to Prod') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("https://data.exactspace.co/deployservice/cicd/${service_type}/$APP_NAME", 'Prod_latest')
                        initialStatus.name = 'Prod_latest'
                        deploymentStatus['Prod_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }*/
        stage('deploying latestimage to UTCL') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("https://eiv8zsdaktab.share.zrok.io/deployservice/cicd/${service_type}/$APP_NAME", 'UTCL_latest')
                        initialStatus.name = 'UTCL_latest'
                        deploymentStatus['UTCL_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }
        /*stage('deploying latestimage to HRD') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("http://40.88.150.243:9001/hrd/deployservice/cicd/${service_type}/$APP_NAME", 'HRD_latest')
                        //def status = deploy("https://hrd-dcs.ngrok.dev/deployservice/cicd/${service_type}/$APP_NAME", 'HRD_latest')
                        initialStatus.name = 'HRD_latest'
                        deploymentStatus['HRD_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }
        stage('deploying latestimage to LPG') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("http://40.88.150.243:9001/lpg/deployservice/cicd/${service_type}/$APP_NAME", 'LPG_latest')
                        //def status = deploy("https://lpg-dcs.ngrok.dev/deployservice/cicd/${service_type}/$APP_NAME", 'LPG_latest')
                        initialStatus.name = 'LPG_latest'
                        deploymentStatus['LPG_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }
        /*stage('deploying latestimage to BHEL') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("https://rmds.bhel.in/deployservice/cicd/${service_type}/$APP_NAME", 'BHEL_latest')
                        initialStatus.name = 'BHEL_latest'
                        deploymentStatus['BHEL_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }*/
     /* stage('check for service status on Prod') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("https://data.exactspace.co/deployservice/cicd/${service_type}/revert/$APP_NAME", 'Prod_revert')
                        revertStatus.name = 'Prod_revert'
                        deploymentStatus['Prod_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }*/
        stage('check for service status on UTCL') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("https://eiv8zsdaktab.share.zrok.io/deployservice/cicd/${service_type}/revert/$APP_NAME", 'UTCL_revert')
                        revertStatus.name = 'UTCL_revert'
                        deploymentStatus['UTCL_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }
        /*stage('check for service status on HRD') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("http://40.88.150.243:9001/hrd/deployservice/cicd/${service_type}/revert/$APP_NAME", 'HRD_revert')
                        //def status = deploy("https://hrd-dcs.ngrok.dev/deployservice/cicd/${service_type}/revert/$APP_NAME", 'HRD_revert')
                        revertStatus.name = 'HRD_revert'
                        deploymentStatus['HRD_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }
        stage('check for service status on LPG') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("http://40.88.150.243:9001/lpg/deployservice/cicd/${service_type}/revert/$APP_NAME", 'LPG_revert')
                        //def status = deploy("https://lpg-dcs.ngrok.dev/deployservice/cicd/${service_type}/revert/$APP_NAME", 'LPG_revert')
                        revertStatus.name = 'LPG_revert'
                        deploymentStatus['LPG_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }
        /*stage('check for service status on BHEL') {
            when {
                 expression { return env.BRANCH_NAME == 'main' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("https://rmds.bhel.in/deployservice/cicd/${service_type}/revert/$APP_NAME", 'BHEL_revert')
                        revertStatus.name = 'BHEL_revert'
                        deploymentStatus['BHEL_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }*/

// Develop Branch 

        stage("Tag old image as d0") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh """
                sudo docker pull $registry/$APP_NAME:d1
                sudo docker tag $registry/$APP_NAME:d1 $registry/$APP_NAME:d0
                sudo docker push $registry/$APP_NAME:d0
                sudo docker pull $registry/qa-smoke-test-es:r1
                """
            }
        }
        stage("get scm for d1"){
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps{
                checkout scmGit(branches: [[name: '*/develop']], extensions: [[$class: 'SubmoduleOption', recursive: true]], userRemoteConfigs: [[url: "git@github.com:exact-space/${repo_name}.git"]])
                
            }
        }
        stage("cython compilation for d1 ") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("gcc for d1") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "gcc index.c -I/usr/include/python3.10/ -Wall -Wextra -O2 -g -o index -lpython3.10"
            }
        }
        /*stage('smoke test before build and deploy') {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                script {
                    def smokeTestStatus = sh(script: 'docker run --rm -v $(pwd):/app dev.exactspace.co/qa-smoke-test-es:r1 python /app/src/smoketest.py', returnStatus: true)
                    echo "Smoke Test Status: ${smokeTestStatus}"
                    if (smokeTestStatus != 0) {
                        def emailAddresses = readFile("${env.WORKSPACE}/mail.txt").trim()
                        if (emailAddresses) {
                            emailext body: "Smoke test before build and deploy failed for ${APP_NAME}. Please check logs.",
                                subject: "Smoke Test for ${currentBuild.fullDisplayName}",
                                to: emailAddresses
                            error "Smoke test before build and deploy failed."
                        }else {
                            error "No email addresses found in mail.txt"
                }
            } else {
                echo "Smoke Test Passed"
            }
        }
        }        
        }*/
        stage("building d1 images") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:d1 ."
            }
        }
        stage("tagging images-d1") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "sudo docker tag $APP_NAME:d1 $registry/$APP_NAME:d1"
            }
        }
        stage("remove old docker image-d1") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "sudo docker image rm $APP_NAME:d1"
            }
        }
        stage("image push-d1") {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                sh "sudo docker push $registry/$APP_NAME:d1"
            }
        }
        stage('deploying latestimage to qa') {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("http://qa.exactspace.co/deployservice/cicd/${service_type}/$APP_NAME", 'qa_latest')
                        initialStatus.name = 'qa_latest'
                        deploymentStatus['qa_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }
        /*stage('smoke test post deployment') {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                script {
                    def smokeTestStatus = sh(script: 'docker run --rm -v $(pwd):/app dev.exactspace.co/qa-smoke-test-es:r1 python /app/src/smoketest.py', returnStatus: true)
                    echo "Smoke Test Status: ${smokeTestStatus}"
                    if (smokeTestStatus != 0) {
                        def emailAddresses = readFile("${env.WORKSPACE}/mail.txt").trim()
                        if (emailAddresses) {
                            emailext body: "smoke test post deployment failed for ${APP_NAME}. Please check logs.",
                                subject: "Smoke Test for ${currentBuild.fullDisplayName}",
                                to: emailAddresses
                            error "smoke test post deployment failed."
                        }else {
                            error "No email addresses found in mail.txt"
                }
            } else {
                echo "Smoke Test Passed"
            }
        }
        }        
        }*/
        stage('check for service status on qa') {
            when {
                 expression { return env.BRANCH_NAME == 'develop' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def revertStatus = deploy("https://qa.exactspace.co/deployservice/cicd/${service_type}/revert/$APP_NAME", 'qa_revert')
                        revertStatus.name = 'qa_revert'
                        deploymentStatus['qa_revert'] = revertStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << revertStatus
                    }
                }
            }
        }

// SANDBOX BRANCH

        stage("get scm for s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: 'sandbox']],
                    userRemoteConfigs: [[url: "git@github.com:exact-space/${repo_name}.git"]]
                ])
            }
        }
        stage("cython compilation for s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("gcc for s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "gcc index.c -I/usr/include/python3.10/ -Wall -Wextra -O2 -g -o index -lpython3.10"
            }
        }
        stage("building images for s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:s1 ."
            }
        }
        stage("tagging images-s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "sudo docker tag $APP_NAME:s1 $registry/$APP_NAME:s1"
            }
        }
        stage("remove old docker image-s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "sudo docker image rm $APP_NAME:s1"
            }
        }
        stage("image push-s1") {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                sh "sudo docker push $registry/$APP_NAME:s1"
            }
        }
        stage('deploying latestimage to sandbox') {
            when {
                 expression { return env.BRANCH_NAME == 'sandbox' }
            }
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def initialStatus = deploy("http://sandbox.exactspace.co/deployservice/cicd/${service_type}/$APP_NAME", 'sandbox_latest')
                        initialStatus.name = 'sandbox_latest'
                        deploymentStatus['sandbox_latest'] = initialStatus ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << initialStatus
                    }
                }
            }
        }
    }
    post {
        always {
            script {
                def buildStartTime = new Date(currentBuild.rawBuild.getTimeInMillis())
                def buildEndTime = new Date(currentBuild.rawBuild.startTimeInMillis + currentBuild.duration)

                def emailAddress = readFile("${env.WORKSPACE}/mail.txt").trim().split("\\s*,\\s*").join(", ")
                if (emailAddress){
                    emailext body: createEmailBody(buildStartTime, buildEndTime, deploymentStatusList, deploymentStatus),
                            subject: "Deployment Status for ${currentBuild.fullDisplayName}",
                            to: emailAddress
                }
            }
        }
    } 
}

def deploy(deploymentUrl, dataCentre) {
    def status = [:]

    timeout(time: 15, unit: 'MINUTES') {
        def response = httpRequest(url: deploymentUrl, timeout: 900000)
        def statusCode = response.status
        def responseBody = response.content

        echo "Response Status Code: ${statusCode}"
        echo "Response Body: ${responseBody}"
        echo "Data Centre: ${dataCentre}"

        // Include response body in status object
        status.responseBody = responseBody

        // Check HTTP response code to determine success or failure
        if (statusCode == 200 ) {
            // Deployment successful
            status.result = 'SUCCESS'
            status.message = "Deployment success ${statusCode} ,"
        } else {
            // Deployment failed
            status.result = 'FAILURE'
            status.message = "Deployment failed ,"
        }

        return status
    }
}

def createEmailBody(buildStartTime, buildEndTime, deploymentStatusList, deploymentStatus) {
    
    def body = "Build triggered at: ${buildStartTime} . . .\n\nDeployment Status: . . .\n\n"
    
    def branchName = env.GIT_BRANCH.replaceAll("refs/heads/", "")
    def dataCenters

    if (branchName == 'main') {
        dataCenters = ['Prod_latest', 'UTCL_latest', 'HRD_latest', 'LPG_latest', 'Prod_revert', 'UTCL_revert', 'HRD_revert', 'LPG_revert']
    } else if (branchName == 'develop') {
        dataCenters = ['qa_latest', 'qa_revert']
    } else if (branchName == 'sandbox') {
        dataCenters = ['sandbox_latest']
    } else {
        dataCenters = []  // Default empty list if branch is not recognized
    }

    dataCenters.each { dataCentre ->
        def status = deploymentStatus[dataCentre]
        if (status) {
            body += "${dataCentre}: \n"
            body += "${status['message']}\n"
            body += "Response Body: ${status['responseBody']} . . .\n\n"
        } else {
            body += "${dataCentre}: No status available . . .\n\n"
        }
    }

    def buildUrl = currentBuild.absoluteUrl
    body += "\nJenkins Build Log: ${buildUrl} . . .\n\n"
    body += "Build ended at: ${buildEndTime}\n"

    return body
}

