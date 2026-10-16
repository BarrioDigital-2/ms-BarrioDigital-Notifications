package cl.duoc.ms_barriodigital_notifications.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Evento que publica ms-requests cuando cambia el estado de una solicitud
 * (exchange solicitudes.direct, routing key solicitud.status).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SolicitudEstadoEvent {

    private Long id;
    private String estadoAnterior;
    private String estadoNuevo;
    private String fecha;
}
