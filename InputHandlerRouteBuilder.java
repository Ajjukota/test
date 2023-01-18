package com.vzw.mtas.be.inputhandler.camel.routes;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.camel.InvalidPayloadException;
import org.apache.camel.LoggingLevel;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.apache.camel.spring.SpringRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

import com.ibm.msg.client.wmq.compat.base.internal.MQC;
import com.vzw.mtas.be.enrichment.camel.strategy.DmdAggregationStrategy;
import com.vzw.mtas.be.enrichment.camel.strategy.OldSimProfileAggregationStrategy;
import com.vzw.mtas.be.enrichment.camel.strategy.SimProfileAggregationStrategy;
import com.vzw.mtas.be.enrichment.service.EnrichmentService;
import com.vzw.mtas.be.enrichment.service.MacIdService;
import com.vzw.mtas.be.enrichment.service.SimProfileGroupService;
import com.vzw.mtas.be.enrichment.utils.AddMDCValues;
import com.vzw.mtas.be.inputhandler.camel.parser.XmlDataFormat;
import com.vzw.mtas.be.inputhandler.camel.utility.interceptors.ClearMDCProcessor;
import com.vzw.mtas.be.inputhandler.redis.config.RedissonSpringDataConfig;
import com.vzw.mtas.be.inputhandler.service.ChangeTrafficQueueService;
import com.vzw.mtas.be.inputhandler.service.CheckTrafficChangeService;
import com.vzw.mtas.be.inputhandler.service.MtasFraudRequestService;
import com.vzw.mtas.be.inputhandler.service.NumberShareService;
import com.vzw.mtas.be.inputhandler.service.OutputQueueService;
import com.vzw.mtas.be.inputhandler.service.RedisService;
import com.vzw.mtas.be.inputhandler_common.constants.CamelConstants;
import com.vzw.mtas.be.inputhandler_common.constants.InputHandlerConstants;
import com.vzw.mtas.be.inputhandler_common.constants.UtilConstants;
import com.vzw.mtas.be.inputhandler_common.exception.FailedValidationException;
import com.vzw.mtas.be.inputhandler_common.exception.MtasException;
import com.vzw.mtas.be.inputhandler_common.exception.MtasInternalException;
import com.vzw.mtas.be.inputhandler_common.exception.MtasRequestException;
import com.vzw.mtas.be.queuing.service.CheckMinChangeService;
import com.vzw.mtas.be.queuing.service.FeatureRemapService;
import com.vzw.mtas.be.queuing.service.MingroupService;
import com.vzw.mtas.be.queuing.service.ProvisionSerializeService;
import com.vzw.mtas.be.queuing.service.QueueingService;
import com.vzw.mtas.be.rulesengine.service.ValidationService;

@Component
public class InputHandlerRouteBuilder extends SpringRouteBuilder {

	final Logger logger = LoggerFactory.getLogger(InputHandlerRouteBuilder.class);

    @Value("${maxRetryAttempt:3}")
    private String maxRetryAttempt;

    @Value("${backOffMultiplier:2}")
    private String backOffMultiplier;

    @Value("${retryAttemptDelay:1000}")
    private String retryAttemptDelay;

	@Autowired
	private XmlDataFormat xmlDataFormat;

	@Autowired
	private ValidationService validationService;

	@Autowired
	private EnrichmentService enrichmentService;

	@Autowired
	private SimProfileGroupService simProfileGroupService;

	@Autowired
	private MacIdService macIdService;

	@Autowired
	private FeatureRemapService featureRemapService;

	@Autowired
	private MingroupService mingroupService;

    @Autowired
    private ProvisionSerializeService provisionSerializeService;

    @Autowired
    private OutputQueueService outputQueueService;

    @Autowired
    private NumberShareService numberShareService;

	@Autowired
	private SimProfileAggregationStrategy simprofileAggregationStrategy;

	@Autowired
	private OldSimProfileAggregationStrategy oldSimprofileAggregationStrategy;

	@Autowired
	private DmdAggregationStrategy dmdAggregationStrategy;

	@Autowired
	private QueueingService queueingService;

	@Autowired
	private CheckMinChangeService checkMinChangeService;

	@Autowired
    private MtasFraudRequestService mtasFraudRequestService;
	
	@Autowired
	private CheckTrafficChangeService checkTrafficChangeService;
	
	@Autowired
	private ChangeTrafficQueueService changeTrafficQueueService;

	@Autowired
	private RedissonSpringDataConfig redissonSpringDataConfig;

	@Autowired
	private RedisService redisService;

	@Override
	public void configure() throws Exception {

        onException(MtasRequestException.class).id("MtasRequestException")
            .log(LoggingLevel.ERROR, logger,
					"MtasRequestException occurred: ${exception.stacktrace}").id("MtasRequestExceptionLog")
            .to(InputHandlerConstants.MTASEXCEPTION_LOG_ROUTE).id("MtasRequestExceptionCommon")
			.handled(true)//Retry not needed
		.end();
        
       onException(FailedValidationException.class).id("FailedValidationException")
    	.log(LoggingLevel.WARN, logger,
			"FailedValidationException occurred: ${exception.stacktrace}").id("FailedValidationExceptionLog")
    	.to(InputHandlerConstants.MTASEXCEPTION_LOG_ROUTE).id("FailedValidationExceptionRoute")
    	.handled(true)//Retry not needed
      .end();

        onException(MtasInternalException.class, ConnectException.class,
				HttpOperationFailedException.class, SocketTimeoutException.class).id("MtasInternalException")
            .log(LoggingLevel.ERROR, logger,"MtasInternalException occurred: ${exception.stacktrace}").id("MtasInternalExceptionLog")
                .onRedelivery((exchange) -> {
                    logger.error("MtasInternalException in MQReaderRouteBuilder route : Retrying");
            })
            .useOriginalMessage()
            .maximumRedeliveries(maxRetryAttempt)
            .redeliveryDelay(retryAttemptDelay)
            .backOffMultiplier(backOffMultiplier)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .logRetryAttempted(true)
            .wireTap(InputHandlerConstants.MTASEXCEPTION_LOG_ROUTE).id("MtasInternalExceptionCommon")
            .end();

        onException(MtasException.class).id("MtasException")
            .log(LoggingLevel.WARN, logger,"MtasException occurred: ${exception.stacktrace}").id("MtasExceptionLog")
            .to(InputHandlerConstants.MTASEXCEPTION_LOG_ROUTE).id("MtasExceptionCommon")
            .handled(true)
            .end();

        //BPVC-6639
		onException(NullPointerException.class).id("NullPointerException")
				.log(LoggingLevel.ERROR, logger,"Exception occurred: ${exception.stacktrace}").id("NullPointerExceptionLog")
				.handled(true)
				.end();

        onException(Exception.class).id("Exception")
            .log(LoggingLevel.ERROR, logger,"Exception occurred: ${exception.stacktrace}").id("ExceptionLog")
//			.to(InputHandlerConstants.MTASEXCEPTION_LOG_ROUTE).id("MtasExceptionCommon")
            .handled(true)
            .end();
        
    	onException(SAXException.class).id("PareseException")
			.log(LoggingLevel.WARN, logger,"Exception occurred: ${exception.stacktrace}").id("SAXExceptionLog")
			.handled(true)
			.end();
		
		onException(InvalidPayloadException.class).id("InvalidPayloadException")
			.log(LoggingLevel.WARN, logger, "Exception occurred: ${exception.stacktrace}").id("ExceptionLog")
			.handled(true)
			.end();

		from("direct:inputHandler")
		.setProperty("reqReceivedTime",constant(new Date()))
		.setProperty("printReceivedTime",constant(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())))
		.log(LoggingLevel.INFO, logger,"Request Received Time: ${exchangeProperty.printReceivedTime}")
		.log(LoggingLevel.INFO, logger,"Received Request XML: ${body}")
		.unmarshal(xmlDataFormat)
		.bean(AddMDCValues.class,"addMDC").id("AddMdcForInputHandler").description("Add Mdc For InputHandler")
		.choice()
		.when(exchangeProperty("trafficChangeRoute"))
		.log(LoggingLevel.INFO, logger,"Change Traffic Route enabled : ${exchangeProperty.trafficChangeRoute}")
		.bean(this.checkTrafficChangeService,"checkTrafficChangeCondition(${exchange},${body},${exchangeProperty.trafficChangeCriteria})")
				.choice()
					.when().simple("${exchangeProperty.neName} == 'MTASINTERNALIH'")
					.to(InputHandlerConstants.MTAS_QUEUEING)
					.endChoice()
				.otherwise()
					.choice()
						.when(exchangeProperty("trafficChangeRoute"))
						.log(LoggingLevel.INFO, logger,"Change Traffic Route enabled : ${exchangeProperty.trafficChangeRoute}")
						.bean(this.checkTrafficChangeService,"checkTrafficChangeCondition(${exchange},${body},${exchangeProperty.trafficChangeCriteria})")
						.choice()
							.when(exchangeProperty("changeRouteTraffic"))
							.log(LoggingLevel.INFO, logger,"Change Traffic criteria matched : ${exchangeProperty.changeRouteTraffic}")
							.bean(this.changeTrafficQueueService,"setChangedTrafficQueueValue(${exchange})")
							.log(LoggingLevel.INFO, logger,"Route traffic to queue : ${exchangeProperty.trafficChangeMQAlias}")
						.otherwise()
							.to(InputHandlerConstants.MTAS_VALIDATION)
						.endChoice()
					.otherwise()
					.to(InputHandlerConstants.MTAS_VALIDATION);
		
			
			// Validation, GL and Enrichment service
			from(InputHandlerConstants.MTAS_VALIDATION)
				.bean(this.validationService,"validateRequest(${body},${exchangeProperty.cacheContainer},${exchangeProperty.neName})")
				.bean(this.simProfileGroupService,UtilConstants.IS_SIM_PROFILE_NEEDED)
				.log(LoggingLevel.DEBUG,"guardedLinkProcessing isSimProfileNeeded: $simple{exchangeProperty.isSimProfileNeeded}")
				.choice()
					.when(exchangeProperty(UtilConstants.IS_SIM_PROFILE_NEEDED).isEqualTo(true))
					.choice().when(exchangeProperty(UtilConstants.IS_OLD_SIM_PROFILE_NEEDED).isEqualTo(true))
						.log(LoggingLevel.DEBUG,"guardedLinkProcessing isSimProfileNeeded: IS_OLD_SIM_PROFILE_NEEDED")
						.enrich(UtilConstants.GL_ROUTE, oldSimprofileAggregationStrategy)
						.endChoice()
					.choice().when(exchangeProperty(UtilConstants.IS_NEW_SIM_PROFILE_NEEDED).isEqualTo(true))
						.log(LoggingLevel.DEBUG,"guardedLinkProcessing isSimProfileNeeded: IS_NEW_SIM_PROFILE_NEEDED")
						.enrich(UtilConstants.GL_ROUTE, simprofileAggregationStrategy)
					.endChoice()
				.end()
				.log(LoggingLevel.DEBUG,"guardedLinkProcessing completed.")

				.bean(this.macIdService ,UtilConstants.IS_DMD_REQUEST)
				.log(LoggingLevel.DEBUG,"DMD isHubRequest: $simple{exchangeProperty.isHubRequest}")
				.choice()
					.when(exchangeProperty(UtilConstants.IS_DMD_REQUEST).isEqualTo(true)).enrich("direct:enrichmentDmd",dmdAggregationStrategy)
						.log(LoggingLevel.INFO,"DMD Processing is required")
						.endChoice()
					.when(exchangeProperty(UtilConstants.IS_DMD_REQUEST).isEqualTo(false))
						.log(LoggingLevel.INFO,"DMD Processing is not required")
					.end()
				.log(LoggingLevel.DEBUG,"Dmd Processing completed.")

				.bean(this.enrichmentService,"enrichRequest")
				.choice()
					.when(exchangeProperty(UtilConstants.HAS_FRAUD_ACTION))
						.split().method(this.mtasFraudRequestService,"generateFraudRequest")
						.parallelProcessing()
						.to(InputHandlerConstants.MTAS_QUEUEING)
						.endChoice()
					.when(exchangeProperty("isNumberShare"))
						.split().method(this.numberShareService,"processNumberShare")
						.parallelProcessing()
						.to(InputHandlerConstants.MTAS_QUEUEING)
						.endChoice()
				.otherwise()
					.to(InputHandlerConstants.MTAS_QUEUEING);
			

				/*** QueueMgr module Routes***/
			from(InputHandlerConstants.MTAS_QUEUEING)
			.bean(AddMDCValues.class,"addMDC")
			.bean(this.featureRemapService , "xmlFeatureRemap(${exchange})")
			.end()
			.bean(this.mingroupService, "determineMinGroup(${exchange})")
				.end()
            .marshal(xmlDataFormat)
			.bean(this.queueingService, "performQueueing(${exchange})")
			.choice()
				.when(exchangeProperty( "isMtasInternalRequired" ))
					.wireTap("direct:mtasInternal")
			.endChoice().end()
			.choice()
				.when(exchangeProperty("minGrpsIdentified"))
					.bean(this.provisionSerializeService, "provisionToJson(${exchange})")
					.bean(this.outputQueueService, "setOutputQueueValues(${exchange})")
					.convertBodyTo(String.class) // body already is string ?
					.removeHeaders("*").id("remove_all_headers")
					.setHeader(InputHandlerConstants.JMS_CORR_ID, exchangeProperty(InputHandlerConstants.CORR_ID)).id("CorrIdSet1")
					.setHeader(InputHandlerConstants.HEADER_TRANSACTION_ID, exchangeProperty(InputHandlerConstants.HEADER_TRANSACTION_ID)).id("SetTransactionHeaderOutput")
					.setHeader(InputHandlerConstants.JMS_DESTINATION_NAME,simple("${exchangeProperty(InputHandlerConstants.OUTPUT_QUEUE_NAME)}")).id("SetDestinationHeaderOutput")
					.setHeader(InputHandlerConstants.JMS_IBM_FORMAT, simple(MQC.MQFMT_STRING)).id("SetIbmPatternHeaderOutput")
					.setHeader(InputHandlerConstants.JMS_IBM_ENCODING, constant(546))
					.setHeader(InputHandlerConstants.JMS_PRIORITY, constant(0))
					.setHeader(InputHandlerConstants.TransactionID,exchangeProperty(CamelConstants.HEADER_TransactionID)).id("SetTransactionalID")

					.toD("${exchangeProperty.outputQueueManagerName}"
							+ ":queue:"
							+ "${exchangeProperty.outputQueueName}"
							+ "?exchangePattern=InOnly"
							+ "&useMessageIDAsCorrelationID=true"
							+ "&explicitQosEnabled=true"
							+ "&priority=0"
						)
					.to("direct:redisGLResponse")
					.log(LoggingLevel.INFO, logger,"InputHandlerRoute :Message Posted Succesfully: ${exchangeProperty.outputQueueName}")
					.log(LoggingLevel.DEBUG, logger,"InputHandlerRoute :Header Posted Succesfully: ${exchangeProperty.HeaderTransactionID}")

			.end()
            .to(UtilConstants.CLEAR_MDC);

        //MDC cleared from logs
        from(UtilConstants.CLEAR_MDC).routeId("CLEARMDCRouteReqReader")
            .process(new ClearMDCProcessor()).id("CLEARMDCProcessorReqReader")
            .end();
        
        from("direct:redisGLResponse")
			.bean(this.redisService, "checkIfRedisEnabled")
			.choice()
				.when(exchangeProperty("redisEnabled"))
				.bean(this.redisService, "addGLResponse(${exchange}, ${exchangeProperty.mtasRequest})")
				.endChoice()
			.end();
        
        from("direct:mtasInternal")
			.bean(AddMDCValues.class,"addMDC")
			.choice()
				.when(exchangeProperty("isMdnChange"))
					.split().method(this.checkMinChangeService,"checkMinChange(${exchange})")
					.parallelProcessing()
					.to(InputHandlerConstants.MTAS_QUEUEING)
			.endChoice()
			.otherwise()
				.to(UtilConstants.CLEAR_MDC);

	}
}
