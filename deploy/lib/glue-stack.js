import * as glue from  '@aws-cdk/aws-glue-alpha';
import { NestedStack,Duration, CfnOutput }  from 'aws-cdk-lib';
import * as iam from "aws-cdk-lib/aws-iam";
import * as dotenv from "dotenv";
dotenv.config();
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class GlueStack extends NestedStack {

    jobArn = '';
    jobName = '';
    rag_jobName = '';
    /**
     *
     * @param {Construct} scope
     * @param {string} id
     * @param {StackProps=} props
     */
    constructor(scope, id, props) {
      super(scope, id, props);


      const connection = new glue.Connection(this, 'GlueJobConnection', {
        type: glue.ConnectionType.NETWORK,
        vpc: props.vpc,
        securityGroups: props.securityGroups,
        subnet:props.subnets[0],
      });


      const ingest_job = new glue.Job(this, 'ingest-knowledge-from-s3',{
            executable: glue.JobExecutable.pythonShell({
            glueVersion: glue.GlueVersion.V1_0,
            pythonVersion: glue.PythonVersion.THREE_NINE,
            script: glue.Code.fromAsset(path.join(__dirname, '../../code/offline_process/aos_write_job.py')),
          }),
          jobName:'ingest_knowledge',
          maxConcurrentRuns:100,
          maxRetries:0,
          connections:[connection],
          maxCapacity:1,
          defaultArguments:{
              '--AOS_ENDPOINT':props.opensearch_endpoint,
              '--REGION':props.region,
              '--AOS_INDEX': "rag-data-src",
              '--additional-python-modules': 'pdfminer.six==20221105,gremlinpython==3.6.3,langchain==0.0.162,beautifulsoup4==4.12.2,boto3>=1.28.52,botocore>=1.31.52,,anthropic_bedrock,python-docx'
          }
      })
      ingest_job.role.addToPrincipalPolicy(
        new iam.PolicyStatement({
              actions: [ 
                "sagemaker:InvokeEndpointAsync",
                "sagemaker:InvokeEndpoint",
                "s3:List*",
                "s3:Put*",
                "s3:Get*",
                "es:*",
                "bedrock:*",
                ],
              effect: iam.Effect.ALLOW,
              resources: ['*'],
              })
      )

      const rag_job = new glue.Job(this, 'ingest-knowledge-from-s3',{
            executable: glue.JobExecutable.pythonShell({
            glueVersion: glue.GlueVersion.V1_0,
            pythonVersion: glue.PythonVersion.THREE_NINE,
            script: glue.Code.fromAsset(path.join(__dirname, '../../code/offline_process/aos_rag_process.py')),
          }),
          jobName:'ingest_knowledge',
          maxConcurrentRuns:100,
          maxRetries:0,
          connections:[connection],
          maxCapacity:1,
          defaultArguments:{
              '--AOS_ENDPOINT':props.opensearch_endpoint,
              '--REGION':props.region,
              '--AOS_INDEX': "rag-data-src",
              '--additional-python-modules': 'pdfminer.six==20221105,gremlinpython==3.6.3,langchain==0.0.162,beautifulsoup4==4.12.2,boto3>=1.28.52,botocore>=1.31.52,,anthropic_bedrock,python-docx'
          }
      })
      rag_job.role.addToPrincipalPolicy(
        new iam.PolicyStatement({
              actions: [ 
                "sagemaker:InvokeEndpointAsync",
                "sagemaker:InvokeEndpoint",
                "s3:List*",
                "s3:Put*",
                "s3:Get*",
                "es:*",
                "bedrock:*",
                ],
              effect: iam.Effect.ALLOW,
              resources: ['*'],
              })
      )
      this.jobArn = ingest_job.jobArn;
      this.jobName = ingest_job.jobName;
      this.rag_jobName = job.rag_jobName
    }
}