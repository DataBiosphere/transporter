variable "pod_security_policy_role" {
  type = string
  description = "Name of the k8s Role in the target cluster which authorizes running pods"
}

variable "db_login_info" {
  type = object({
    url = string,
    db_name = string,
    username = string,
    password_secret_name = string,
    password_secret_key = string
  })
  description = "Login info which the Transporter Manager should use when connecting to Postgres"
}

variable "kafka_url" {
  type = string
  description = "Bootstrap URL of the Kafka cluster the Transporter Manager should connect into"
}

variable "kafka_topics" {
  type = object({
    request_topic = string,
    progress_topic = string,
    result_topic = string
  })
  description = "Names of the Kafka topics the Transporter Manager should connect to"
}

variable "kafka_tls_info" {
  type = object({secret_name = string, secret_key = string})
  description = "Identifiers for a k8s secret containing the TLS certificate for Kafka"
}

variable "kafka_login_info" {
  type = object({
    username = string,
    password_scram_algorithm = string,
    password_secret_name = string,
    password_secret_key = string
  })
  description = "Login info which the Transporter Manager should use when connecting to Kafka"
}
