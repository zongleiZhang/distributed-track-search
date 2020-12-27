package com.ada.geometry;

import com.ada.QBSTree.RectElem;
import java.io.Serializable;
import java.util.Objects;

/**
 * 点无序
 */
public class Segment extends RectElem implements Serializable {
    public TrackPoint p1;
    public TrackPoint p2;
    public int hashCode;

    public Segment(){}

    public Segment(TrackPoint p1, TrackPoint p2){
        super(p1,p2);
        if (p1.timestamp > p2.timestamp){
            this.p2 = p1;
            this.p1 = p2;
        }else {
            this.p1 = p1;
            this.p2 = p2;
        }
        hashCode = Objects.hash(p1, p2);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Segment)) return false;
        Segment segment = (Segment) o;
        return p1.equals(segment.p1) &&
                p2.equals(segment.p2);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public Segment clone()  {
        Segment segment = (Segment) super.clone();
        segment.p1 =  p1.clone();
        segment.p2 =  p2.clone();
        return segment;
    }

}
