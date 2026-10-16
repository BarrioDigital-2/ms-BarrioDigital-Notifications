package cl.duoc.ms_barriodigital_notifications.consumer.solicitud;

import cl.duoc.ms_barriodigital_notifications.config.RabbitMQConfig;
import cl.duoc.ms_barriodigital_notifications.consumer.ConsumidorBase;
import cl.duoc.ms_barriodigital_notifications.dto.SolicitudEstadoEvent;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Dominio solicitudes: notifica al vecino que su solicitud cambio de estado.
 * Cola solicitudes.estados (exchange Direct, routing key solicitud.status).
 * El JSON llega ya convertido a SolicitudEstadoEvent por el JacksonJsonMessageConverter.
 */
@Service
public class SolicitudEstadoConsumer extends ConsumidorBase {

    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_ESTADOS_QUEUE)
    public void onSolicitudEstado(SolicitudEstadoEvent evento,
                                  Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            validar(evento.getId() != null, "El evento de cambio de estado no trae id");

            log.info("Notificación: solicitud #{} cambió de {} a {}",
                    evento.getId(), evento.getEstadoAnterior(), evento.getEstadoNuevo());

            confirmar(channel, deliveryTag);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_ESTADOS_QUEUE, e);
        }
    }
}
