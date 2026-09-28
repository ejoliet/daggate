// Paste into your Jenkinsfile stages block.
stage('DAG Production Gate') {
    steps {
        // Zero deps: any agent with Python 3.10+ works.
        sh 'python3 daggate.py dags/ --format text'
    }
    // Optional: publish JSON for trend dashboards
    // sh 'python3 daggate.py dags/ --format json > daggate-report.json || true'
    // archiveArtifacts artifacts: 'daggate-report.json'
}
