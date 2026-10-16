package cl.duoc.ms_barriodigital_notifications.consumer.solicitud;

import cl.duoc.ms_barriodigital_notifications.config.RabbitMQConfig;
import cl.duoc.ms_barriodigital_notifications.consumer.ConsumidorBase;
import cl.duoc.ms_barriodigital_notifications.dto.SolicitudCreadaEvent;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Dominio solicitudes: notifica al vecino que su solicitud fue ingresada.
 * Cola solicitudes.creadas (exchange Direct, routing key solicitud.created).
 * El JSON llega ya convertido a SolicitudCreadaEvent por el JacksonJsonMessageConverter.
 */
@Service
public class SolicitudCreadaConsumer extends ConsumidorBase {

    @RabbitListener(queues = RabbitMQConfig.SOLICITUDES_CREADAS_QUEUE)
    public void onSolicitudCreada(SolicitudCreadaEvent evento,
                                  Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            validar(evento.getId() != null, "El evento de solicitud creada no trae id");

            log.info("Notificación: nueva solicitud #{} de tipo {} para {}",
                    evento.getId(), evento.getTipoTramite(), evento.getVecinoEmail());

            confirmar(channel, deliveryTag);
        } catch (Exception e) {
            rechazar(channel, deliveryTag, RabbitMQConfig.SOLICITUDES_CREADAS_QUEUE, e);
        }
    }
}
