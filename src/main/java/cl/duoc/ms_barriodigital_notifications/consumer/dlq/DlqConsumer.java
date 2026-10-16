package cl.duoc.ms_barriodigital_notifications.consumer.dlq;

import cl.duoc.ms_barriodigital_notifications.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Dominio DLQ: monitor de las 3 Dead Letter Queues.
 *
 * Recibe el Message crudo (no un DTO) porque a la DLQ llegan justamente los mensajes
 * que pueden venir mal formados. Los registra con WARN y hace ACK manual para sacarlos de la DLQ.
 */
@Slf4j
@Service
public class DlqConsumer {

    @RabbitListener(
            queues = {
                    RabbitMQConfig.SOLICITUDES_CREADAS_DLQ,
                    RabbitMQConfig.SOLICITUDES_ESTADOS_DLQ,
                    RabbitMQConfig.SOLICITUDES_NOTIFICACIONES_DLQ
            },
            // Factory sin conversor JSON ni reintentos (ver RabbitMQConfig.dlqContainerFactory)
            containerFactory = "dlqContainerFactory")
    public void onMensajeMuerto(Message message, Channel channel) throws IOException {
        String queueName = message.getMessageProperties().getConsumerQueue();
        String mensaje = new String(message.getBody(), StandardCharsets.UTF_8);

        log.warn("Mensaje muerto recibido en DLQ {}: {}", queueName, mensaje);
        log.warn("     -> Motivo: {}", motivo(message));

        // ACK manual: el mensaje ya quedo registrado, se saca de la DLQ
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    /**
     * RabbitMQ agrega el header x-death al mensaje muerto.
     * reason = "rejected" (basicNack) o "expired" (TTL de 30 s vencido), y la cola de origen.
     */
    private String motivo(Message message) {
        Object xDeath = message.getMessageProperties().getHeaders().get("x-death");
        if (xDeath instanceof List<?> muertes && !muertes.isEmpty() && muertes.get(0) instanceof Map<?, ?> muerte) {
            return muerte.get("reason") + " (cola de origen: " + muerte.get("queue") + ")";
        }
        return "desconocido";
    }
}
