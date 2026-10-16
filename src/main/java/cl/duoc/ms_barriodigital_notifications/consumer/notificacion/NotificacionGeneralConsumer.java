package cl.duoc.ms_barriodigital_notifications.consumer.notificacion;

import cl.duoc.ms_barriodigital_notifications.config.RabbitMQConfig;
import cl.duoc.ms_barriodigital_notifications.consumer.ConsumidorBase;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * Dominio notificaciones: registro general de todos los eventos de solicitudes.
 * Cola solicitudes.notificaciones (exchange Topic, patron solicitud.#).
 * Por esta cola llegan ambos tipos de evento, por eso se recibe como Map
 * (el converter igual deserializa el JSON).
 */
@Service
public class NotificacionGeneralConsumer extends ConsumidorBase {

    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_NOTIFICACIONES_QUEUE)
    public void onNotificacionGeneral(Map<String, Object> evento,
                                      Channel channel,
                                      @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                                      @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) throws IOException {
        try {
            validar(evento.get("id") != null, "El evento general no trae id");

            log.info("Notificación general recibida [{}]: {}", routingKey, evento);

            confirmar(channel, deliveryTag);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_NOTIFICACIONES_QUEUE, e);
        }
    }
}
