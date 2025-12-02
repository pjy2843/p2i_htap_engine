terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
    random = { source = "hashicorp/random" }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "ap-northeast-2"
}

variable "key_pair_name" {
  type = string
}

resource "random_id" "bucket_id" {
  byte_length = 4
}

resource "aws_s3_bucket" "iceberg_bucket" {
  bucket = "p2i-iceberg-${random_id.bucket_id.hex}"
  acl    = "private"
  force_destroy = true
}

resource "aws_iam_role" "ec2_role" {
  name = "p2i-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role_policy.json
}

data "aws_iam_policy_document" "ec2_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name = "p2i-ec2-s3"
  role = aws_iam_role.ec2_role.id
  policy = data.aws_iam_policy_document.s3_policy.json
}

data "aws_iam_policy_document" "s3_policy" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = ["${aws_s3_bucket.iceberg_bucket.arn}", "${aws_s3_bucket.iceberg_bucket.arn}/*"]
  }
}