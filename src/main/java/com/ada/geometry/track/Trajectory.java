package com.ada.geometry.track;

import com.ada.common.ArrayQueue;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackInfo;
import com.ada.geometry.TrackPoint;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Trajectory implements Serializable, TrackInfo {
    public ArrayQueue<Segment> elms;
    public Integer TID;


    public Trajectory(ArrayQueue<Segment> elms,
                      int TID){
        this.elms = elms;
        this.TID = TID;
    }

    public void addSegments(List<Segment> value) {
        elms.addAll(value);
    }

    public List<Segment> addTrackPoints(List<TrackPoint> points) {
        List<Segment> list = new ArrayList<>(points.size()-1);
        Segment segment = new Segment(elms.getLast().p2, points.get(0));
        list.add(segment);
        elms.add(segment);
        for (int i = 1; i < points.size(); i++) {
            segment = new Segment(points.get(i-1), points.get(i));
            elms.add(segment);
            list.add(segment);
        }
        return list;
    }

    /**
     * 移除过时采样点
     * @param time 时间标准
     * @return 0 本轨迹的所有采样点都过时， 1移除过时采样点后还有超过一个元素保留
     */
    public List<Segment> removeElem(long time){
        List<Segment> timeOutElem = new ArrayList<>();
        while(!elms.isEmpty() && elms.element().p2.getTimestamp() < time) {
            timeOutElem.add(elms.remove());
        }
        return timeOutElem;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Trajectory)) return false;
        Trajectory that = (Trajectory) o;
        return TID.equals(that.TID);
    }

    @Override
    public int hashCode() {
        return TID.hashCode();
    }

    @Override
    public int obtainTID() {
        return TID;
    }

    public TrackPoint getPoint(int i) {
        if (i < elms.size()){
            return elms.get(i).p1;
        }else if (i == elms.size()){
            return elms.getLast().p2;
        }else {
            return null;
        }
    }
}















