package cl.duoc.ms_barriodigital_notifications.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Evento que publica ms-requests cuando se crea una solicitud
 * (exchange solicitudes.direct, routing key solicitud.created).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SolicitudCreadaEvent {

    private Long id;
    private String tipoTramite;
    private String vecinoEmail;
    private String estado;
    private String fecha;
}
