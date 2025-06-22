variable "enabled" {
  type    = bool
  default = true
}

variable "name" {}
variable "namespace" {}
variable "chart" {}
variable "repo" {}
variable "values_files" {
  type    = list(string)
  default = []
}
variable "chart_version" {
  type    = string
  default = null
}


resource "helm_release" "this" {
  count             = var.enabled ? 1 : 0
  name              = var.name
  namespace         = var.namespace
  chart             = var.chart
  repository        = var.repo
  version           = var.chart_version
  create_namespace  = true
  values            = [for file in var.values_files : file(file)]
  timeout           = 600
  wait              = false 
}
