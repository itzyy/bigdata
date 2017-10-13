package cn.kepuchina.trident.spout;

import java.io.Serializable;

/**
 * Created by Zouyy on 2017/8/9.
 */
public class DiagnosisEvent implements Serializable {

    private static final long serialVersionUID=1l;

    public double lat;
    public double lng;
    public long time;
    public String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        super();
        this.lat = lat;
        this.lng = lng;
        this.time = time;
        this.diagnosisCode = diagnosisCode;
    }
}
