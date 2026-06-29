using Amazon.CDK;

var app = new App();
new SportsPipeline.Infra.SportsPipelineStack(app, "SportsPipelineStack", new StackProps());
app.Synth();
