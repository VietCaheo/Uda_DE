"""     13. Readshift ETL: 
        -> using S3 as a staging area, but for very small Data -> we can copy it directly from the EC2 machine.
        -> Building a Redshift Cluster: 
            AWS manual document for Redshift Cluster: https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html
            `Redshift has to act as an user who has Read access to S3`
        -> Detail steps for lauching RedShift Cluster and Creat DB.
            + Security: accessible from where (virtual private cloud or Jupiter workspace)
            + Access to S3 (cluster needs to access an s3 bucket)
        -> Infrastructure as Code: (`IaC`)
            + Options to achieve IaC on AWS (aws-cli scripts/ AWS sdk/ Amazon Cloud Platform)
            + -> IaC advantages: Sharing/ Reproducibility/ Multiple Deployment/Maintainability
    18. Enabling `Programmatic Access` to IaC:
        -> IaC choice: 
            + use the python AWS SDK aka boto3
            + user called: `dwhadmin`
    19. Demo IaC:
        Creating Redshift Cluster
        Creating a new IAM role and Attaching Policy
        Open an incoming TCP port to access the Cluster endpoint 

    25. Optimizing the table
        WHen a table is partitioned up into many pieces and distributed across slices in different machines, this is done by blindly.
        The two possible strategies are:
            + Distribution style (EVEN/ ALL/ AUTO/ KEY)
            + Sorting key
        
"""
