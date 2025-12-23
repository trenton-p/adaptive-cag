import os
import pathlib
import aws_cdk as cdk

from aws_cdk import (
    aws_s3 as _s3,
    aws_lambda as _lambda,
    aws_s3_deployment as _deployment,
    aws_cloudfront as _cdn,
    aws_cloudfront_origins as _origins,
    aws_logs as _logs
)
from constructs import Construct

class StaticWebsite(Construct):

    def __init__(self, scope: Construct, id: str, fn_url: _lambda.FunctionUrl, form_api: str) -> None:
        super().__init__(scope, id)

        # Create the static website bucket
        website_bucket = _s3.Bucket(
            self,
            "WebsiteBucket",
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create a separate website log bucket
        log_bucket = _s3.Bucket(
            self,
            "WebsiteLogs",
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            object_ownership=_s3.ObjectOwnership.BUCKET_OWNER_PREFERRED
        )

        # Create the website CDN
        self.distribution = _cdn.Distribution(
            self,
            "CloudFrontDistribution",
            comment="CDN for the ACME News Website.",
            default_root_object="index.html",
            error_responses=[
                _cdn.ErrorResponse(
                    http_status=404,
                    response_http_status=404,
                    response_page_path="/404.html",
                    ttl=cdk.Duration.minutes(30)
                )
            ],
            default_behavior=_cdn.BehaviorOptions(
                origin=_origins.S3BucketOrigin.with_origin_access_control(website_bucket),
                allowed_methods=_cdn.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                viewer_protocol_policy=_cdn.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                compress=True,
                cache_policy=_cdn.CachePolicy.CACHING_OPTIMIZED,
            ),
            additional_behaviors={
                "/api/chat": _cdn.BehaviorOptions(
                    origin=_origins.FunctionUrlOrigin.with_origin_access_control(fn_url),
                    allowed_methods=_cdn.AllowedMethods.ALLOW_ALL,
                    cache_policy=_cdn.CachePolicy.CACHING_DISABLED,
                    viewer_protocol_policy=_cdn.ViewerProtocolPolicy.HTTPS_ONLY,
                    origin_request_policy=_cdn.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    compress=False
                ),
                "/api/contact": _cdn.BehaviorOptions(
                    origin=_origins.HttpOrigin(
                        domain_name=f"{form_api}.execute-api.{cdk.Aws.REGION}.{cdk.Aws.URL_SUFFIX}"
                    ),
                    allowed_methods=_cdn.AllowedMethods.ALLOW_ALL,
                    cache_policy=_cdn.CachePolicy.CACHING_DISABLED,
                    viewer_protocol_policy=_cdn.ViewerProtocolPolicy.HTTPS_ONLY
                )
            },
            minimum_protocol_version=_cdn.SecurityPolicyProtocol.TLS_V1_2_2021,
            price_class=_cdn.PriceClass.PRICE_CLASS_100,
            http_version=_cdn.HttpVersion.HTTP2_AND_3,
            enable_logging=True,
            log_bucket=log_bucket,
            log_file_prefix="cloudfront-logs",
            log_includes_cookies=True
        )

        # Deploy the HTML content for the static website
        _deployment.BucketDeployment(
            self,
            "WebsiteDeployment",
            sources=[
                _deployment.Source.asset(
                    path=str(pathlib.Path(__file__).parent.joinpath("public").resolve())
                )
            ],
            destination_bucket=website_bucket,
            distribution=self.distribution,
            retain_on_delete=False
        )


    # @classmethod
    # def _create_log_group(self, scope: Construct, log_name: str):
    #     _logs.LogGroup(
    #         scope=scope,
    #         id=log_name,
    #         log_group_name=f"/aws/lambda/{log_name}",
    #         retention=_logs.RetentionDays.ONE_DAY,
    #         removal_policy=cdk.RemovalPolicy.DESTROY
    #     )


    @property
    def domain_name(self):
        return self.distribution.distribution_domain_name