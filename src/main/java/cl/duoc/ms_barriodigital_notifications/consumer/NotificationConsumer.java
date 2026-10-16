package cl.duoc.ms_barriodigital_notifications.consumer;

import cl.duoc.ms_barriodigital_notifications.config.RabbitMQConfig;
import cl.duoc.ms_barriodigital_notifications.dto.SolicitudCreadaEvent;
import cl.duoc.ms_barriodigital_notifications.dto.SolicitudEstadoEvent;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * Consumidores principales de eventos de solicitudes, con ACK manual.
 *
 * - Exito -> channel.basicAck: RabbitMQ elimina el mensaje de la cola.
 * - Error -> channel.basicNack(requeue=false): el mensaje va al DLX de su cola y termina en la DLQ.
 *
 * El JSON del mensaje llega ya convertido al DTO por el JacksonJsonMessageConverter (ver RabbitMQConfig).
 */
@Slf4j
@Service
public class NotificationConsumer {

    // ========== SOLICITUD CREADA (Direct: solicitud.created) ==========
    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_CREADAS_QUEUE)
    public void onSolicitudCreada(SolicitudCreadaEvent evento,
                                  Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            validar(evento.getId() != null, "El evento de solicitud creada no trae id");

            log.info("Notificación: nueva solicitud #{} de tipo {} para {}",
                    evento.getId(), evento.getTipoTramite(), evento.getVecinoEmail());

            // ACK manual: el mensaje se proceso correctamente
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_CREADAS_QUEUE, e);
        }
    }

    // ========== CAMBIO DE ESTADO (Direct: solicitud.status) ==========
    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_ESTADOS_QUEUE)
    public void onSolicitudEstado(SolicitudEstadoEvent evento,
                                  Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            validar(evento.getId() != null, "El evento de cambio de estado no trae id");

            log.info("Notificación: solicitud #{} cambió de {} a {}",
                    evento.getId(), evento.getEstadoAnterior(), evento.getEstadoNuevo());

            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_ESTADOS_QUEUE, e);
        }
    }

    // ========== TODOS LOS EVENTOS (Topic: solicitud.#) ==========
    /**
     * Por esta cola llegan ambos tipos de evento, asi que se recibe como Map
     * (el converter igual deserializa el JSON).
     */
    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_NOTIFICACIONES_QUEUE)
    public void onNotificacionGeneral(Map<String, Object> evento,
                                      Channel channel,
                                      @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                                      @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) throws IOException {
        try {
            validar(evento.get("id") != null, "El evento general no trae id");

            log.info("Notificación general recibida [{}]: {}", routingKey, evento);

            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_NOTIFICACIONES_QUEUE, e);
        }
    }

    // ========== HELPERS ==========

    /** Regla minima de negocio: un evento sin id no se puede notificar. */
    private void validar(boolean condicion, String mensaje) {
        if (!condicion) {
            throw new IllegalArgumentException(mensaje);
        }
    }

    /**
     * NACK sin reencolar (requeue=false): RabbitMQ envia el mensaje al DLX configurado
     * en la cola (x-dead-letter-exchange) y de ahi a su DLQ.
     */
    private void rechazar(Channel channel, long deliveryTag, String cola, Exception e) throws IOException {
        log.error("Error procesando mensaje de {}: {}. Se envía a la DLQ.", cola, e.getMessage());
        channel.basicNack(deliveryTag, false, false);
    }
}
