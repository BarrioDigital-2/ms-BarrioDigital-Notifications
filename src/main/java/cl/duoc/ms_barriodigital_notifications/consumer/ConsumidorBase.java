package cl.duoc.ms_barriodigital_notifications.consumer;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Base comun de los consumidores de eventos: concentra el manejo de confirmaciones (ACK manual)
 * para que cada consumidor solo se ocupe de su dominio.
 *
 * - confirmar -> channel.basicAck: el mensaje se proceso bien y RabbitMQ lo elimina de la cola.
 * - rechazar  -> channel.basicNack(requeue=false): el mensaje va al DLX de su cola y termina en la DLQ.
 */
public abstract class ConsumidorBase {

    /** Logger con el nombre del consumidor concreto (ej. SolicitudCreadaConsumer). */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** ACK manual: confirma solo este mensaje (multiple=false). */
    protected void confirmar(Channel channel, long deliveryTag) throws IOException {
        channel.basicAck(deliveryTag, false);
    }

    /**
     * NACK sin reencolar (requeue=false): RabbitMQ envia el mensaje al DLX configurado
     * en la cola (x-dead-letter-exchange) y de ahi a su DLQ.
     * Se usa para errores no recuperables: reintentar el mismo mensaje volveria a fallar.
     */
    protected void rechazar(Channel channel, long deliveryTag, String cola, Exception e) throws IOException {
        log.error("Error procesando mensaje de {}: {}. Se envía a la DLQ.", cola, e.getMessage());
        channel.basicNack(deliveryTag, false, false);
    }

    /** Regla minima de negocio: un evento sin id no se puede notificar. */
    protected void validar(boolean condicion, String mensaje) {
        if (!condicion) {
            throw new IllegalArgumentException(mensaje);
        }
    }
}
