output "transporter_manager_k8s_manifest" {
  type = string
  value = templatefile("${path.module}/k8s-template.yaml", {
    service_account = "transporter-manager-sa",
    pod_security_policy_role = "use-pod-security-policy-role",

    db_url = var.db_login_info.url,
    db_name = var.db_login_info.db_name,
    db_username = var.db_login_info.username,
    db_password_secret_name = var.db_login_info.password_secret_name,
    db_password_secret_key = var.db_login_info.password_secret_key,

    kafka_url = var.kafka_url,
    kafka_tls_secret_name = var.kafka_tls_info.secret_name,
    kafka_tls_secret_key = var.kafka_tls_info.secret_key,
    kafka_username = var.kafka_login_info.username,
    kafka_password_secret_name = var.kafka_login_info.password_secret_name,
    kafka_password_secret_key = var.kafka_login_info.password_secret_key,
    kafka_password_scram_algorithm = var.kafka_login_info.password_scram_algorithm,
    requests_topic = var.kafka_topics.request_topic,
    progress_topic = var.kafka_topics.progress_topic,
    results_topic = var.kafka_topics.result_topic
  })
}
