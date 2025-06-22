# Terraform configuration to set up VPC + EKS (converted from CloudFormation and extended with EKS resources)

provider "aws" {
   region = var.aws_region   
}



terraform {
 backend "s3" {
    bucket         = "rca-tfstate-dev" 
    key            = "terraform.tfstate"     
    region          = "us-east-1"  
    encrypt        = true
 }
}

#-------------------------------------------------------------
# VPC
#-------------------------------------------------------------
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name_prefix}-vpc"
    Project = "${var.project_name}"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.name_prefix}-igw"
    Project = "${var.project_name}"
  }
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.name_prefix}-nat"
    Project = "${var.project_name}"
  }

  depends_on = [aws_internet_gateway.igw]
}

resource "aws_eip" "nat" {
  vpc = true
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.name_prefix}-public-rt"
    Project = "${var.project_name}"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }

  tags = {
    Name = "${var.name_prefix}-private-rt"
    Project = "${var.project_name}"
  }
}

resource "aws_subnet" "public" {
  count                   = length(var.public_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  map_public_ip_on_launch = true
  availability_zone       = var.azs[count.index]

  tags = {
    Name = "${var.name_prefix}-public-${count.index}"
    Project = "${var.project_name}"
  }
}

resource "aws_subnet" "private" {
  count             = length(var.private_subnet_cidrs)
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.azs[count.index]

  tags = {
    Name = "${var.name_prefix}-private-${count.index}"
    Project = "${var.project_name}"
  }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

#-------------------------------------------------------------
# IAM Roles for EKS
#-------------------------------------------------------------
resource "aws_iam_role" "eks_cluster" {
  name = "${var.name_prefix}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Principal = {
        Service = "eks.amazonaws.com"
      },
      Effect = "Allow",
      Sid    = ""
    }]
  })

 tags = {
    Project = "${var.project_name}"
  }

}

resource "aws_iam_role_policy_attachment" "eks_cluster" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"

}

resource "aws_iam_role" "eks_node_group" {
  name = "${var.name_prefix}-eks-node-group-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Principal = {
        Service = "ec2.amazonaws.com"
      },
      Effect = "Allow",
      Sid    = ""
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_ebs_csi_driver" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

#-------------------------------------------------------------
# IAM role for EBS CSI driver service account (IRSA)
#-------------------------------------------------------------
data "tls_certificate" "oidc_thumbprint" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc_thumbprint.certificates[0].sha1_fingerprint]
}

resource "aws_iam_role" "ebs_csi_sa" {
  name = "${var.name_prefix}-ebs-csi-sa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "sts:AssumeRoleWithWebIdentity",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Condition = {
       StringEquals = {
       "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:kube-system:ebs-csi-controller-sa"
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi_sa" {
  role       = aws_iam_role.ebs_csi_sa.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}



resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Delete"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  depends_on = [
    null_resource.wait_for_cluster,
    helm_release.ebs_csi_driver
  ]
}


resource "helm_release" "ebs_csi_driver" {
  name       = "aws-ebs-csi-driver"
  namespace  = "kube-system"
  repository = "https://kubernetes-sigs.github.io/aws-ebs-csi-driver"
  chart      = "aws-ebs-csi-driver"
  version    = "2.30.0" # Use latest compatible version

  set {
    name  = "controller.serviceAccount.create"
    value = true
  }

  set {
    name  = "controller.serviceAccount.name"
    value = "ebs-csi-controller-sa"
  }

  # Add this line to use the IRSA role
 set_sensitive {
  name  = "controller.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
  value = aws_iam_role.ebs_csi_sa.arn
}

  depends_on = [
    null_resource.wait_for_cluster,
    aws_iam_role_policy_attachment.ebs_csi_sa
  ]
}


resource "aws_iam_role_policy_attachment" "eks_worker_node" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_cni" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_registry" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role" "airflow_s3_access_sa" {
  name = "${var.name_prefix}-airflow-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "sts:AssumeRoleWithWebIdentity",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Condition = {
       StringEquals = {
       "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:airflow:airflow-worker-test"
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_s3_access_sa" {
  role       = aws_iam_role.airflow_s3_access_sa.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role" "airflow_web_s3_access_sa" {
  name = "${var.name_prefix}-airflow-web-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "sts:AssumeRoleWithWebIdentity",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Condition = {
       StringEquals = {
       "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:airflow:airflow-webserver-test"
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_web_s3_access_sa" {
  role       = aws_iam_role.airflow_web_s3_access_sa.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role" "airflow_scheduler_s3_access_sa" {
  name = "${var.name_prefix}-airflow-scheduler-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "sts:AssumeRoleWithWebIdentity",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Condition = {
       StringEquals = {
       "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:airflow:airflow-scheduler-test"
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_scheduler_s3_access_sa" {
  role       = aws_iam_role.airflow_scheduler_s3_access_sa.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}


resource "aws_iam_role" "dask_s3_access_sa" {
  name = "${var.name_prefix}-dask-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "sts:AssumeRoleWithWebIdentity",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Condition = {
       StringEquals = {
       "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:dask:dask-access"
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}
resource "kubernetes_namespace" "dask" {
  metadata {
    name = "dask"
  }
}

# Kubernetes ServiceAccount with annotation
resource "kubernetes_service_account" "dask_access" {
  metadata {
    name      = "dask-access"
    namespace = "dask"
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.dask_s3_access_sa.arn
    }
  }
  depends_on = [kubernetes_namespace.dask]
}

resource "aws_iam_role_policy_attachment" "dask_s3_access_sa" {
  role       = aws_iam_role.dask_s3_access_sa.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

#-------------------------------------------------------------
# EKS Cluster
#-------------------------------------------------------------
resource "aws_eks_cluster" "main" {
  name     = "${var.name_prefix}-eks"
  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids = aws_subnet.private[*].id
  }
 tags = {
    Project = "${var.project_name}"
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster
  ]
}

#-------------------------------------------------------------
# EKS Node Group
#-------------------------------------------------------------
resource "aws_eks_node_group" "default" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.name_prefix}-eks-ng"
  node_role_arn   = aws_iam_role.eks_node_group.arn
  subnet_ids      = aws_subnet.private[*].id
  scaling_config {
    desired_size = 2
    max_size     = 3
    min_size     = 1
  }
  instance_types = ["m5.xlarge"]

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_registry
  ]
}


#-------------------------------------------------------------
# Ensure Kubernetes availability before running Helm charts
#-------------------------------------------------------------
resource "null_resource" "wait_for_cluster" {
  depends_on = [aws_eks_node_group.default]

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for EKS cluster to be fully available..."
      aws eks --region ${var.aws_region} update-kubeconfig --name ${aws_eks_cluster.main.name}
      
      # Wait for nodes to be ready
      echo "Waiting for EKS nodes to be ready..."
      kubectl wait --for=condition=ready nodes --all --timeout=600s

      echo "EKS cluster is now ready for deployments"
    EOT
  }
}

#-------------------------------------------------------------
#Cluster Services
#-------------------------------------------------------------
module "airflow" {
  source           = "./modules/helm_release"
  count            = var.install_airflow ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on       = [aws_iam_role_policy_attachment.airflow_s3_access_sa,null_resource.wait_for_cluster]

  enabled          = true
  name             = "airflow"
  namespace        = "airflow"
  chart            = "airflow"
  repo             = "https://airflow.apache.org"
  values_files     = ["${path.module}/values/airflow-values.yaml"]
  chart_version    = "1.16.0"
}
module "clearml" {
  source       = "./modules/helm_release"
  count        = var.install_clearml ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on   = [null_resource.wait_for_cluster]

  enabled      = true
  name         = "clearml"
  namespace    = "clearml"
  chart        = "clearml"
  repo         = "https://clearml.github.io/clearml-helm-charts"
  values_files = ["${path.module}/values/clearml-values.yaml"]
  chart_version = "7.14.4"
}

module "prometheus" {
  source       = "./modules/helm_release"
  count        = var.install_prometheus ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on   = [null_resource.wait_for_cluster]

  enabled      = true
  name         = "prometheus"
  namespace    = "monitoring"
  chart        = "prometheus"
  repo         = "https://prometheus-community.github.io/helm-charts"
  values_files = ["${path.module}/values/prometheus-values.yaml"]
  chart_version = "25.18.0"
}

module "grafana" {
  source       = "./modules/helm_release"
  count        = var.install_grafana ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on   = [null_resource.wait_for_cluster]

  enabled      = true
  name         = "grafana"
  namespace    = "monitoring"
  chart        = "grafana"
  repo         = "https://grafana.github.io/helm-charts"
  values_files = ["${path.module}/values/grafana-values.yaml"]
  chart_version = "7.3.0"
}

module "dask" {
  source       = "./modules/helm_release"
  count        = var.install_dask ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on   = [null_resource.wait_for_cluster,kubernetes_service_account.dask_access]

  enabled      = true
  name         = "dask"
  namespace    = "dask"
  chart        = "dask"
  repo         = "https://helm.dask.org"
  values_files = ["${path.module}/values/dask-values.yaml"]
  chart_version = "2024.1.1"
}

module "mlflow" {
  source         = "./modules/helm_release"
  count          = var.install_mlflow ? 1 : 0
  
  # Add explicit dependency on cluster readiness
  depends_on     = [null_resource.wait_for_cluster]

  enabled        = true
  name           = "mlflow"
  namespace      = "mlflow"
  chart          = "mlflow"
  repo           = "https://community-charts.github.io/helm-charts"
  chart_version  = "0.7.3"
  values_files   = ["${path.module}/values/mlflow-values.yaml"]
}