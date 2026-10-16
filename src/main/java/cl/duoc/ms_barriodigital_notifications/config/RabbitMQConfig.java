package cl.duoc.ms_barriodigital_notifications.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declaracion centralizada de toda la topologia RabbitMQ de BarrioDigital.
 *
 * <pre>
 * solicitudes.direct (Direct) --solicitud.created--> solicitudes.creadas        --DLX--> solicitudes.creadas.dlx (Fanout)        --> solicitudes.creadas.dlq
 * solicitudes.direct (Direct) --solicitud.status---> solicitudes.estados        --DLX--> solicitudes.estados.dlx (Fanout)        --> solicitudes.estados.dlq
 * solicitudes.topic  (Topic)  --solicitud.#--------> solicitudes.notificaciones --DLX--> solicitudes.notificaciones.dlx (Fanout) --> solicitudes.notificaciones.dlq
 * </pre>
 *
 * Al arrancar, el RabbitAdmin de Spring Boot declara en el broker todos los beans de este archivo.
 */
@Slf4j
@Configuration
public class RabbitMQConfig {

    // ========== EXCHANGES PRINCIPALES ==========
    public static final String SOLICITUDES_DIRECT_EXCHANGE = "solicitudes.direct";
    public static final String SOLICITUDES_TOPIC_EXCHANGE = "solicitudes.topic";

    // ========== COLAS PRINCIPALES ==========
    public static final String SOLICITUDES_CREADAS_QUEUE = "solicitudes.creadas";
    public static final String SOLICITUDES_ESTADOS_QUEUE = "solicitudes.estados";
    public static final String SOLICITUDES_NOTIFICACIONES_QUEUE = "solicitudes.notificaciones";

    // ========== ROUTING KEYS ==========
    public static final String SOLICITUD_CREATED_ROUTING_KEY = "solicitud.created";
    public static final String SOLICITUD_STATUS_ROUTING_KEY = "solicitud.status";
    /** Topic: "#" captura cero o mas palabras -> solicitud.created, solicitud.status, etc. */
    public static final String SOLICITUD_ALL_ROUTING_KEY = "solicitud.#";

    // ========== DLX (Dead Letter Exchanges) ==========
    public static final String SOLICITUDES_CREADAS_DLX = "solicitudes.creadas.dlx";
    public static final String SOLICITUDES_ESTADOS_DLX = "solicitudes.estados.dlx";
    public static final String SOLICITUDES_NOTIFICACIONES_DLX = "solicitudes.notificaciones.dlx";

    // ========== DLQ (Dead Letter Queues) ==========
    public static final String SOLICITUDES_CREADAS_DLQ = "solicitudes.creadas.dlq";
    public static final String SOLICITUDES_ESTADOS_DLQ = "solicitudes.estados.dlq";
    public static final String SOLICITUDES_NOTIFICACIONES_DLQ = "solicitudes.notificaciones.dlq";

    // ========== ROUTING KEYS DE MENSAJES MUERTOS ==========
    public static final String SOLICITUD_CREATED_DEAD_ROUTING_KEY = "solicitud.created.dead";
    public static final String SOLICITUD_STATUS_DEAD_ROUTING_KEY = "solicitud.status.dead";
    public static final String SOLICITUD_NOTIFICACION_DEAD_ROUTING_KEY = "solicitud.notificacion.dead";

    // ========== PARAMETROS DE LAS COLAS ==========
    /** Un mensaje que nadie consume en 30 segundos expira y se va a la DLQ. */
    private static final int MESSAGE_TTL_MS = 30_000;
    /** Los mensajes muertos se conservan 24 horas para auditoria. */
    private static final int DLQ_TTL_MS = 86_400_000;

    // ========== CONVERSOR JSON ==========

    /**
     * Convierte JSON <-> objetos Java. Spring Boot lo aplica a los @RabbitListener
     * (deserializa el body al DTO del parametro) y al RabbitTemplate.
     * JacksonJsonMessageConverter es el reemplazo de Jackson2JsonMessageConverter
     * en Spring AMQP 4 (Spring Boot 4 usa Jackson 3).
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    // ========== RECUPERACION TRAS AGOTAR REINTENTOS ==========

    /**
     * Si el listener lanza una excepcion (ej. JSON invalido que no se puede convertir al DTO),
     * Spring reintenta con backoff (ver application.properties). Al agotar los reintentos se llama a este recoverer.
     *
     * Con acknowledge-mode=manual, el recoverer por defecto no hace NACK y el mensaje quedaria "unacked" para siempre.
     * Lanzando AmqpRejectAndDontRequeueException con rejectManual=true, el contenedor hace basicNack(requeue=false)
     * y el mensaje se va a la DLQ de su cola.
     */
    @Bean
    public MessageRecoverer rechazarADlqRecoverer() {
        return (message, causa) -> {
            log.error("Reintentos agotados para el mensaje de {} ({}). Se envía a la DLQ.",
                    message.getMessageProperties().getConsumerQueue(), motivoDelFallo(causa));
            throw new AmqpRejectAndDontRequeueException("Reintentos agotados, se envía a la DLQ", true, causa);
        };
    }

    /** Resume la causa en una frase corta para el log (la traza completa no aporta en este caso). */
    private static String motivoDelFallo(Throwable causa) {
        for (Throwable t = causa; t != null; t = t.getCause()) {
            if (t instanceof MessageConversionException) {
                return "el contenido no es un JSON valido";
            }
        }
        return causa.getMessage();
    }

    // ========== CONTAINER FACTORY PARA LAS DLQ ==========

    /**
     * Los consumidores de DLQ reciben justamente los mensajes que pueden venir mal formados.
     * Si usaran el conversor JSON, un body invalido haria fallar al propio monitor de la DLQ
     * y el mensaje se perderia. Esta factory:
     * - hereda la configuracion de application.properties (ACK manual, prefetch 1),
     * - usa SimpleMessageConverter (no interpreta el body),
     * - no reintenta (el monitor solo registra y hace ACK).
     */
    @Bean
    public SimpleRabbitListenerContainerFactory dlqContainerFactory(SimpleRabbitListenerContainerFactoryConfigurer configurer,
                                                                    ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setMessageConverter(new SimpleMessageConverter());
        factory.setAdviceChain();
        return factory;
    }

    // ========== EXCHANGES PRINCIPALES ==========

    /** Direct: cada routing key llega solo a la cola enlazada con esa misma key. */
    @Bean
    public DirectExchange solicitudesDirectExchange() {
        return new DirectExchange(SOLICITUDES_DIRECT_EXCHANGE, true, false);
    }

    /** Topic: enruta por patron, permite una cola que escucha todos los eventos de solicitud. */
    @Bean
    public TopicExchange solicitudesTopicExchange() {
        return new TopicExchange(SOLICITUDES_TOPIC_EXCHANGE, true, false);
    }

    // ========== COLAS PRINCIPALES (CON TTL Y DLX) ==========

    @Bean
    public Queue solicitudesCreadasQueue() {
        return colaConDeadLetter(SOLICITUDES_CREADAS_QUEUE, SOLICITUDES_CREADAS_DLX, SOLICITUD_CREATED_DEAD_ROUTING_KEY);
    }

    @Bean
    public Queue solicitudesEstadosQueue() {
        return colaConDeadLetter(SOLICITUDES_ESTADOS_QUEUE, SOLICITUDES_ESTADOS_DLX, SOLICITUD_STATUS_DEAD_ROUTING_KEY);
    }

    @Bean
    public Queue solicitudesNotificacionesQueue() {
        return colaConDeadLetter(SOLICITUDES_NOTIFICACIONES_QUEUE, SOLICITUDES_NOTIFICACIONES_DLX,
                SOLICITUD_NOTIFICACION_DEAD_ROUTING_KEY);
    }

    // ========== BINDINGS PRINCIPALES ==========

    @Bean
    public Binding solicitudesCreadasBinding() {
        return BindingBuilder.bind(solicitudesCreadasQueue())
                .to(solicitudesDirectExchange())
                .with(SOLICITUD_CREATED_ROUTING_KEY);
    }

    @Bean
    public Binding solicitudesEstadosBinding() {
        return BindingBuilder.bind(solicitudesEstadosQueue())
                .to(solicitudesDirectExchange())
                .with(SOLICITUD_STATUS_ROUTING_KEY);
    }

    @Bean
    public Binding solicitudesNotificacionesBinding() {
        return BindingBuilder.bind(solicitudesNotificacionesQueue())
                .to(solicitudesTopicExchange())
                .with(SOLICITUD_ALL_ROUTING_KEY);
    }

    // ========== DEAD LETTER EXCHANGES (FANOUT) ==========
    // Fanout: ignora la routing key y entrega a todas las colas enlazadas (su DLQ).

    @Bean
    public FanoutExchange solicitudesCreadasDlx() {
        return new FanoutExchange(SOLICITUDES_CREADAS_DLX, true, false);
    }

    @Bean
    public FanoutExchange solicitudesEstadosDlx() {
        return new FanoutExchange(SOLICITUDES_ESTADOS_DLX, true, false);
    }

    @Bean
    public FanoutExchange solicitudesNotificacionesDlx() {
        return new FanoutExchange(SOLICITUDES_NOTIFICACIONES_DLX, true, false);
    }

    // ========== DEAD LETTER QUEUES ==========

    @Bean
    public Queue solicitudesCreadasDlq() {
        return colaMuerta(SOLICITUDES_CREADAS_DLQ);
    }

    @Bean
    public Queue solicitudesEstadosDlq() {
        return colaMuerta(SOLICITUDES_ESTADOS_DLQ);
    }

    @Bean
    public Queue solicitudesNotificacionesDlq() {
        return colaMuerta(SOLICITUDES_NOTIFICACIONES_DLQ);
    }

    // ========== BINDINGS DLQ -> DLX ==========

    @Bean
    public Binding solicitudesCreadasDlqBinding() {
        return BindingBuilder.bind(solicitudesCreadasDlq()).to(solicitudesCreadasDlx());
    }

    @Bean
    public Binding solicitudesEstadosDlqBinding() {
        return BindingBuilder.bind(solicitudesEstadosDlq()).to(solicitudesEstadosDlx());
    }

    @Bean
    public Binding solicitudesNotificacionesDlqBinding() {
        return BindingBuilder.bind(solicitudesNotificacionesDlq()).to(solicitudesNotificacionesDlx());
    }

    // ========== FABRICAS DE COLAS ==========

    /**
     * Cola principal durable:
     * 1. x-message-ttl: el mensaje expira a los 30 s si nadie lo consume.
     * 2. x-dead-letter-exchange: si expira o se rechaza (basicNack sin requeue), va a su DLX.
     * 3. x-dead-letter-routing-key: routing key con la que llega al DLX.
     */
    private Queue colaConDeadLetter(String nombre, String dlx, String deadRoutingKey) {
        return QueueBuilder.durable(nombre)
                .withArgument("x-message-ttl", MESSAGE_TTL_MS)
                .withArgument("x-dead-letter-exchange", dlx)
                .withArgument("x-dead-letter-routing-key", deadRoutingKey)
                .build();
    }

    /** DLQ durable: guarda los mensajes fallidos 24 horas para auditoria y analisis. */
    private Queue colaMuerta(String nombre) {
        return QueueBuilder.durable(nombre)
                .withArgument("x-message-ttl", DLQ_TTL_MS)
                .build();
    }
}
