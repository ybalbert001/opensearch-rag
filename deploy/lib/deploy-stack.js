// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import { Stack, Duration, CfnOutput, RemovalPolicy } from "aws-cdk-lib";
import { DockerImageFunction }  from 'aws-cdk-lib/aws-lambda';
import { DockerImageCode,Architecture } from 'aws-cdk-lib/aws-lambda';
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as ecr from 'aws-cdk-lib/aws-ecr';
import { VpcStack } from './vpc-stack.js';
import {GlueStack} from './glue-stack.js';
import {OpenSearchStack} from './opensearch-stack.js';
import { Ec2Stack } from "./ec2-stack.js";
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as dotenv from "dotenv";
dotenv.config();

import path from "path";
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

import { join } from "path";
export class DeployStack extends Stack {
  /**
   *
   * @param {Construct} scope
   * @param {string} id
   * @param {StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    const region = props.env.region;
    const account_id = Stack.of(this).account;
    const aos_existing_endpoint = props.env.aos_existing_endpoint;

    const vpcStack = new VpcStack(this,'vpc-stack',{env:process.env});
    const vpc = vpcStack.vpc;
    const subnets = vpcStack.subnets;
    const securityGroups = vpcStack.securityGroups;

    const cn_region = ["cn-north-1","cn-northwest-1"];

    let opensearch_endpoint=aos_existing_endpoint;
    if (process.env.aos_required!=='false'){
      let opensearchStack;
      const ec2stack = new Ec2Stack(this,'Ec2Stack',{vpc:vpc,securityGroup:securityGroups[0]});
      new CfnOutput(this, 'OpenSearch EC2 Proxy Address', { value: `http://${ec2stack.publicIP}/_dashboards/`});
      ec2stack.addDependency(vpcStack);
      if (!aos_existing_endpoint || aos_existing_endpoint === 'optional'){
        opensearchStack = new OpenSearchStack(this,'os-chat-dev',
             {vpc:vpc,subnets:subnets,securityGroup:securityGroups[0]});
       opensearch_endpoint = opensearchStack.domainEndpoint;
       opensearchStack.addDependency(vpcStack);
      // if (!aos_existing_endpoint || aos_existing_endpoint === 'optional'){
      //   opensearchStack = new OpenSearchServerlessStack(this,'os-chat-serverless',
      //        {vpc:vpc,subnets:subnets,securityGroup:securityGroups[0]});
      //  opensearch_endpoint = opensearchStack.domainEndpoint;
      //  opensearchStack.addDependency(vpcStack);
      // }
       new CfnOutput(this,'opensearch endpoint',{value:opensearch_endpoint});
   }
    }

    new CfnOutput(this,'VPC',{value:vpc.vpcId});
    new CfnOutput(this,'region',{value:process.env.CDK_DEFAULT_REGION});
    new CfnOutput(this,'UPLOAD_BUCKET',{value:process.env.UPLOAD_BUCKET});

    //glue job
    const gluestack = new GlueStack(this,'glue-stack',{opensearch_endpoint,region,vpc,subnets,securityGroups});
    new CfnOutput(this, `Glue Ingest Job name`,{value:`${gluestack.jobName}`});
    new CfnOutput(this, `Glue RAG Job name`,{value:`${gluestack.rag_jobName}`});
    gluestack.addDependency(vpcStack)
  }
}
