using System.Configuration;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;

namespace CosmosOptimization
{
    /// </summary>
    class LoggingHandler : RequestHandler
    {
        private static readonly string AppInsightsKey = ConfigurationManager.AppSettings["AppInsightsKey"];

        private readonly TelemetryClient telemetryClient;

        public LoggingHandler()
        {
            TelemetryConfiguration configuration = TelemetryConfiguration.CreateDefault();
            configuration.InstrumentationKey = AppInsightsKey;
            this.telemetryClient = new TelemetryClient(configuration);
            this.telemetryClient.InstrumentationKey = AppInsightsKey;
        }

        public override async Task<ResponseMessage> SendAsync(
            RequestMessage request,
            CancellationToken cancellationToken)
        {

            using (Microsoft.ApplicationInsights.Extensibility.IOperationHolder<RequestTelemetry> operation = this.telemetryClient.StartOperation<RequestTelemetry>("CosmosDBRequest"))
            {
                this.telemetryClient.TrackTrace($"{request.Method.Method} - {request.RequestUri.ToString()}");
                ResponseMessage response = await base.SendAsync(request, cancellationToken);

                operation.Telemetry.ResponseCode = ((int)response.StatusCode).ToString();
                operation.Telemetry.Success = response.IsSuccessStatusCode;

                var requestContent = "";

                if (request.Content != null)
                {
                    // Parse out what the request was
                    StreamReader reader = new StreamReader(request.Content);
                    requestContent = reader.ReadToEnd();
                }

                operation.Telemetry.Properties.Add("RUCharge", response.Headers.RequestCharge.ToString()); //log RU of operation
                operation.Telemetry.Properties.Add("RequestContent", requestContent);
                operation.Telemetry.Properties.Add("ResourceUri", request.RequestUri.ToString());

                this.telemetryClient.StopOperation(operation);
                return response;
            }
        }
    }
}


