package com.ada.flinkFunction;


import com.ada.Hausdorff.Hausdorff;
import com.ada.Hausdorff.SimilarState;
import com.ada.QBSTree.LocalTree;
import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.common.collections.Collections;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.geometry.track.TrackHauOne;
import com.ada.model.globalToLocal.*;
import com.ada.model.queryResult.QueryResult;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class HausdorffLocalPF extends ProcessWindowFunction<G2LElem, QueryResult, Integer, TimeWindow> {
    private int subTask;
    private boolean hasInit;
    private int count;

    private Map<Long, TwoTIDs> tIDsMap;
    private Map<Integer, TrackHauOne> passTrackMap;
    private Map<Integer, TrackHauOne> topKTrackMap;
    private LocalTree<Segment> segmentIndex;
    private LocalTree<TrackHauOne> pruneIndex;

    private Map<Integer, List<TrackPoint>> addPassPoints; //0
    private Map<Integer, List<TrackPoint>> addTopKPoints; //1
    private List<G2LPoints> addPassTracks; //2
    private List<G2LPoints> addTopKTracks; //3
    private Set<Integer> delPassTIDs;  //4
    private Set<Integer> delTopKTIDs;  //5
    private List<Integer> convertPassTIDs;  //6
    private List<Integer> convertTopKTIDs;  //7
    private List<G2LElem> adjustInfo; //8-13
    private G2LElem newRegion;    //14
    private List<G2LPoints> verifyPass;
    private List<G2LPoints> verifyTopK;
    private Collector<QueryResult> out;
    private long winStart;

    private TwoTIDs inTwoTIDs, outTwoTIDs, inAndOutTwoTIDs;
    private RoaringBitmap allAlterTIDs;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<G2LElem> elements,
                        Collector<QueryResult> out) throws Exception {
        this.out = out;
        this.winStart = context.window().getStart();
        classifyElements(elements);

        //记录无采样点滑出，但其topK结果可能发生变化的轨迹
        Set<TrackHauOne> pruneChangeTracks = new HashSet<>();
        preElements(pruneChangeTracks);
        long logicWinStart = context.window().getEnd() - Constants.windowSize * Constants.logicWindow;
        countTIDs(pruneChangeTracks, logicWinStart);
        //移除滑出的点
        Set<Integer> passEmptyTIDs = new HashSet<>();
        Set<Integer> topKEmptyTIDs = new HashSet<>();
        removeSegment(logicWinStart, passEmptyTIDs, topKEmptyTIDs);

        dealAllSlideOutTracks(pruneChangeTracks, passEmptyTIDs, topKEmptyTIDs);
        processUpdatedTrack(pruneChangeTracks);
        for (TrackHauOne track : pruneChangeTracks) {
            if (track.candidateInfo.size() < Constants.topK + Constants.t) {
                dealCandidateSmall(track);
            } else {
                changeThreshold(track);
            }
        }
        for (G2LPoints glPoints : addPassTracks) {
            TrackHauOne track = passTrackMap.get(glPoints.TID);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, true, false);
            pruneIndex.insert(track);
            mayBeAnotherTopK(track);
        }
        for (G2LPoints glPoints : addTopKTracks) {
            TrackHauOne track = topKTrackMap.get(glPoints.TID);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, false, false);
            pruneIndex.insert(track);
        }
        for (Integer TID : outTwoTIDs.passTIDs) mayBeAnotherTopK(passTrackMap.get(TID));
        for (Integer TID : inAndOutTwoTIDs.passTIDs) mayBeAnotherTopK(passTrackMap.get(TID));
        for (Integer TID : convertTopKTIDs) mayBeAnotherTopK(passTrackMap.get(TID));

        if (newRegion != null) {
            if (!hasInit)
                startUseSubTask();
            if (newRegion.value == null) {
                discardSubTask();
            } else {
                adjustRegion();
            }
            newRegion = null;
        }
        check();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= subTask; i++) {
            sb.append("\t\t");
        }
        sb.append(subTask).append(":").append(count);
        System.out.println(sb.toString());
        closeWindows();
    }

    private void closeWindows() {
        addPassPoints.clear();
        addTopKPoints.clear();
        addPassTracks.clear();
        addTopKTracks.clear();
        delPassTIDs.clear();
        delTopKTIDs.clear();
        convertPassTIDs.clear();
        convertTopKTIDs.clear();
        if (adjustInfo.size() != 0)
            adjustInfo.clear();
        verifyPass.clear();
        verifyTopK.clear();
    }

    private boolean check() {
        if (topKTrackMap == null)
            throw new IllegalArgumentException();
        for (Integer TID : topKTrackMap.keySet()) {
            if (passTrackMap.containsKey(TID))
                throw new IllegalArgumentException();
        }
        if (passTrackMap.size() != verifyPass.size()) {
            difference(verifyPass, passTrackMap);
            throw new IllegalArgumentException();
        }
        if (topKTrackMap.size() != verifyTopK.size()) {
            difference(verifyTopK, topKTrackMap);
            throw new IllegalArgumentException();
        }
        if (!verifyTrack(verifyPass, passTrackMap))
            throw new IllegalArgumentException();
        if (!verifyTrack(verifyTopK, topKTrackMap))
            throw new IllegalArgumentException();
        if (!checkPruneIndex())
            throw new IllegalArgumentException();
        if (!checkSegmentIndex())
            throw new IllegalArgumentException();
        for (TrackHauOne track : passTrackMap.values()) {
            if (!checkTrack(track, true))
                throw new IllegalArgumentException();
        }
        for (TrackHauOne track : topKTrackMap.values()) {
            if (!checkTrack(track, false))
                throw new IllegalArgumentException();
        }
        return true;
    }

    private void difference(List<G2LPoints> verifyPass, Map<Integer, TrackHauOne> passTrackMap) {
        if (verifyPass.size() < passTrackMap.size()){
            Set<Integer> TIDs = new HashSet<>(Collections.changeCollectionElem(verifyPass, gl -> gl.TID));
            passTrackMap.entrySet().removeIf(entry -> TIDs.contains(entry.getKey()));
        }else {
            verifyPass.removeIf(gl -> passTrackMap.containsKey(gl.TID));
        }
    }

    private boolean verifyTrack(List<G2LPoints> verifyTopK, Map<Integer, TrackHauOne> topKTrackMap) {
        for (G2LPoints glTrack : verifyTopK) {
            TrackHauOne track = topKTrackMap.get(glTrack.TID);
            List<TrackPoint> list = Segment.segmentsToPoints(track.trajectory.elms);
            if (list.size() != glTrack.points.size())
                return false;
            for (int i = 0; i < list.size(); i++) {
                if (!list.get(i).equals(glTrack.points.get(i)))
                    return false;
            }
        }
        return true;
    }

    private boolean checkTrack(TrackHauOne track, boolean isPass) {
        Integer TID = track.trajectory.TID;

        if (isPass){
            for (Segment segment : track.trajectory.elms) {
                Long mapKey = ((segment.getSecondTime()/Constants.windowSize)) * Constants.windowSize;
                if (!tIDsMap.get(mapKey).passTIDs.contains(TID))
                    throw new IllegalArgumentException();
            }
        }else {
            for (Segment segment : track.trajectory.elms) {
                Long mapKey = ((segment.getSecondTime()/Constants.windowSize)) * Constants.windowSize;
                if (!tIDsMap.get(mapKey).topKTIDs.contains(TID))
                    throw new IllegalArgumentException();
            }
        }

        //RelatedInfo 检查
        for (SimilarState key : track.getRelatedInfo().keySet()) {
            SimilarState state = track.getRelatedInfo().get(key);
            int comparedTID = state.getStateAnoTID(TID);
            TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
            if (isPass && comparedTrack == null) comparedTrack = topKTrackMap.get(comparedTID);
            if (comparedTrack == null)
                throw new IllegalArgumentException();
            if (key != state)
                throw new IllegalArgumentException();
            if (comparedTrack.getSimilarState(TID) != state)
                throw new IllegalArgumentException();
            if (!SimilarState.isEquals(state, Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory)))
                throw new IllegalArgumentException();
            if (!comparedTrack.candidateInfo.contains(TID) && !track.candidateInfo.contains(comparedTID))
                throw new IllegalArgumentException();
        }

        //candidateInfo 检查
        if (track.candidateInfo.size() < Constants.topK)
            throw new IllegalArgumentException();
        for (Integer comparedTID : track.candidateInfo) {
            if(track.getSimilarState(comparedTID) == null)
                throw new IllegalArgumentException();
        }
        for (int i = 0; i < track.candidateInfo.size()-1; i++) {
            SimilarState state1 = track.getSimilarState(track.candidateInfo.get(i));
            SimilarState state2 = track.getSimilarState(track.candidateInfo.get(i+1));
            if (Double.compare(state1.distance, state2.distance) > 0)
                throw new IllegalArgumentException();
        }

        Rectangle MBR = track.getPruningRegion(0.0);
        Rectangle pruneArea = MBR.clone().extendLength(track.threshold);
        double dis0 = track.getKCanDistance(Constants.topK).distance;
        double dis1 = track.getKCanDistance(Constants.topK + Constants.t*2).distance;
        if (track.threshold > dis1 || track.threshold < dis0)
            throw new IllegalArgumentException();

        //rect 检查
        if (!pruneArea.equals(track.rect))
            throw new IllegalArgumentException();
        Set<Integer> selectedTIDs = segmentIndex.getInternalNoIPTIDs(pruneArea);
        selectedTIDs.remove(TID);
        if (!Collections.collectionsEqual(selectedTIDs, track.candidateInfo))
            throw new IllegalArgumentException();
        return true;
    }

    private boolean checkSegmentIndex() {
        List<Segment> list = segmentIndex.getAllElement();
        int total = 0;
        for (TrackHauOne track : passTrackMap.values())
            total += track.trajectory.elms.size();
        if (total != list.size())
            throw new IllegalArgumentException();
        for (Segment segment : list) {
            if (!passTrackMap.get(segment.getTID()).trajectory.elms.contains(segment))
                throw new IllegalArgumentException();
        }
        return true;
    }

    private boolean checkPruneIndex() {
        List<TrackHauOne> tracks = pruneIndex.getAllElement();
        if (tracks.size() != passTrackMap.size()+topKTrackMap.size())
            throw new IllegalArgumentException();
        for (TrackHauOne track : tracks) {
            TrackHauOne compareTrack = passTrackMap.get(track.trajectory.TID);
            if (compareTrack == null)
                compareTrack = topKTrackMap.get(track.trajectory.TID);
            if (compareTrack != track)
                throw new IllegalArgumentException();
        }
        return true;
    }

    private void adjustRegion() {
        Rectangle newRectangle = (Rectangle) newRegion.value;
        Map<Integer, List<G2LValue>> adjInfoMap = new HashMap<>(6);
        for (int i = 8; i <= 13; i++) 
            adjInfoMap.put(i, new ArrayList<>());
        for (G2LElem elem : adjustInfo)
            adjInfoMap.get((int) elem.flag).add(elem.value);
        if (adjInfoMap.get(10).size() > (passTrackMap.size()*6.0/10.0) ||
                passTrackMap.size() == 0){ //删除的经过轨迹太多重新构建
            reBuiltSubTask(adjInfoMap);
        }else { //删除的轨迹不多，在原有数据的基础上重建
            updateSubTask(adjInfoMap);
        }
    }

    private void updateSubTask(Map<Integer, List<G2LValue>> adjInfoMap) {
        //topK结果可能发生变化的轨迹TID
        Set<TrackHauOne> pruneChangeTracks = new HashSet<>();

        //12：(调整负责区域)新增经过轨迹
        adjInfoMap.get(12).forEach(value -> addPassTrack((G2LPoints) value));
        //13：(调整负责区域)新增topK轨迹
        adjInfoMap.get(13).forEach(value -> addTopKTrack((G2LPoints) value));
        //11： (调整负责区域)删除topK轨迹
        adjInfoMap.get(11).forEach(value -> removeTopKTrack(((G2TID) value).TID));
        //10： (调整负责区域)删除经过轨迹
        adjInfoMap.get(10).forEach(value -> removePassTrack(pruneChangeTracks, ((G2TID) value).TID));
        //8： (调整负责区域)经过轨迹改为topK轨迹
        adjInfoMap.get(8).forEach(value -> convertPassTrack(pruneChangeTracks, ((G2TID) value).TID));
        //9： (调整负责区域)topK轨迹改为经过轨迹
        adjInfoMap.get(9).forEach(value -> convertTopKTrack(((G2TID) value).TID));

        for (TrackHauOne track : pruneChangeTracks) {
            if (track.candidateInfo.size() < Constants.topK + Constants.t){ //候选轨迹太少了
                dealCandidateSmall(track);
            }else { //候选轨迹足够
                changeThreshold(track);
            }
        }

        //12：(调整负责区域)新增经过轨迹
        for (G2LValue value : adjInfoMap.get(12)) {
            Integer TID = ((G2LPoints) value).TID;
            TrackHauOne track = passTrackMap.get(TID);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, true, false);
            pruneIndex.insert(track);
            mayBeAnotherTopK(track);
        }
        //13：(调整负责区域)新增topK轨迹
        for (G2LValue value : adjInfoMap.get(13)) {
            Integer TID = ((G2LPoints) value).TID;
            TrackHauOne track = topKTrackMap.get(TID);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, false, false);
            pruneIndex.insert(track);
        }
        //9： (调整负责区域)topK轨迹改为经过轨迹
        adjInfoMap.get(9).forEach(value -> mayBeAnotherTopK(passTrackMap.get(((G2TID) value).TID)));
    }

    private void reBuiltSubTask(Map<Integer, List<G2LValue>> adjInfoMap) {
        //8： (调整负责区域)经过轨迹改为topK轨迹
        for (G2LValue info : adjInfoMap.get(8)) {
            int TID = ((G2TID) info).TID;
            topKTrackMap.put(TID, passTrackMap.remove(TID));
        }
        //9： (调整负责区域)topK轨迹改为经过轨迹
        for (G2LValue info : adjInfoMap.get(9)) {
            int TID = ((G2TID) info).TID;
            passTrackMap.put(TID, topKTrackMap.remove(TID));
        }
        //10： (调整负责区域)删除经过轨迹
        adjInfoMap.get(10).forEach(value -> passTrackMap.remove(((G2TID) value).TID));
        //11： (调整负责区域)删除topK轨迹
        adjInfoMap.get(11).forEach(value -> topKTrackMap.remove(((G2TID) value).TID));
        //12：(调整负责区域)新增经过轨迹
        adjInfoMap.get(12).forEach(value -> {
            G2LPoints glPoints = (G2LPoints) value;
            TrackHauOne track = glPoints.toTrackHauOne();
            passTrackMap.put(glPoints.TID, track);
        });
        //13：(调整负责区域)新增topK轨迹
        adjInfoMap.get(13).forEach(value -> {
            G2LPoints glPoints = (G2LPoints) value;
            TrackHauOne track = glPoints.toTrackHauOne();
            topKTrackMap.put(glPoints.TID, track);
        });
        passTrackMap.values().forEach(TrackHauOne::clear);
        topKTrackMap.values().forEach(TrackHauOne::clear);
        tIDsMap.values().forEach(TwoTIDs::clear);

        List<Segment> elms = new ArrayList<>();
        for (TrackHauOne track : passTrackMap.values()) {
            int TID = track.trajectory.TID;
            elms.addAll(track.trajectory.elms);
            for (Segment segment : track.trajectory.elms) {
                Long mapKey = ((int) (segment.getSecondTime()/Constants.windowSize)) * Constants.windowSize;
                tIDsMap.computeIfAbsent(mapKey, aLong -> new TwoTIDs()).passTIDs.add(TID);
            }
        }
        for (TrackHauOne track : topKTrackMap.values()) {
            int TID = track.trajectory.TID;
            for (Segment segment : track.trajectory.elms) {
                Long mapKey = ((int) (segment.getSecondTime()/Constants.windowSize)) * Constants.windowSize;
                tIDsMap.computeIfAbsent(mapKey, aLong -> new TwoTIDs()).topKTIDs.add(TID);
            }
        }
        Rectangle region = Rectangle.pointsMBR(elms.toArray(new Point[0]));
        segmentIndex.rebuildRoot(elms, region);

        List<TrackHauOne> tracks = new ArrayList<>(passTrackMap.size() + topKTrackMap.size());
        for (TrackHauOne track : passTrackMap.values()) {
            tracks.add(track);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, true, false);
        }
        for (TrackHauOne track : topKTrackMap.values()) {
            tracks.add(track);
            Rectangle MBR = track.rect.clone();
            track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone().extendLength(Constants.extend), segmentIndex, passTrackMap, false, false);
        }
        region = Rectangle.pointsMBR(tracks.toArray(new Point[0]));
        pruneIndex.rebuildRoot(tracks, region);
    }

    private void discardSubTask() {
        hasInit = false;
        tIDsMap = null;
        passTrackMap = null;
        topKTrackMap = null;
        segmentIndex = null;
        pruneIndex = null;
    }

    /**
     * 轨迹track可能成为其它轨迹的topK结果
     */
    private void mayBeAnotherTopK(TrackHauOne track) {
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        List<Integer> list = pruneIndex.trackInternal(MBR);
        Integer TID = track.trajectory.TID;
        list.remove(TID);
        for (Integer comparedTid : list) {
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            if (!comparedTrack.candidateInfo.contains(TID)){
                comparedTrack.addTrackCandidate(track);
                comparedTrack.updateCandidateInfo(TID);
                if (comparedTrack.getKCanDistance(Constants.topK + Constants.t*2).distance < comparedTrack.threshold) {
                    double newThreshold = comparedTrack.getKCanDistance(Constants.topK + Constants.t).distance;
                    Rectangle pruneArea = comparedTrack.rect.clone().extendLength(newThreshold - comparedTrack.threshold);
                    DTConstants.cacheTighten(comparedTrack, passTrackMap,
                            t -> !pruneArea.isInternal(t.rect.clone().extendLength(-t.threshold)));
                    comparedTrack.threshold = newThreshold;
                    pruneIndex.alterELem(comparedTrack, pruneArea);
                }
            }
        }
    }

    private void removeSegment(long logicWinStart, Set<Integer> passEmptyTIDs, Set<Integer> topKEmptyTIDs) {
        DTConstants.removeSegment(outTwoTIDs.passTIDs, inAndOutTwoTIDs.passTIDs, logicWinStart, passEmptyTIDs, segmentIndex, passTrackMap);
        for (Integer TID : outTwoTIDs.topKTIDs) {
            TrackHauOne track = topKTrackMap.get(TID);
            track.trajectory.removeElem(logicWinStart);
            if (track.trajectory.elms.size() == 0) topKEmptyTIDs.add(TID);
        }
        for (Integer TID : inAndOutTwoTIDs.topKTIDs) {
            topKTrackMap.get(TID).trajectory.removeElem(logicWinStart);
        }
    }

    private void countTIDs(Set<TrackHauOne> pruneChangeTracks, long logicWinStart) throws CloneNotSupportedException {
        inTwoTIDs = tIDsMap.get(winStart).clone();
        addPassTracks.forEach(glPoints -> inTwoTIDs.passTIDs.remove(glPoints.TID));
        addTopKTracks.forEach(glPoints -> inTwoTIDs.topKTIDs.remove(glPoints.TID));
        outTwoTIDs = tIDsMap.remove(logicWinStart - Constants.windowSize);
        if (outTwoTIDs == null)
            outTwoTIDs = new TwoTIDs();
        allAlterTIDs = RoaringBitmap.or(inTwoTIDs.passTIDs, inTwoTIDs.topKTIDs,
                outTwoTIDs.passTIDs, outTwoTIDs.topKTIDs);
        pruneChangeTracks.removeIf(track -> allAlterTIDs.contains(track.trajectory.TID));
        inAndOutTwoTIDs = inTwoTIDs.clone();
        inAndOutTwoTIDs.and(outTwoTIDs);
        inTwoTIDs.andNot(inAndOutTwoTIDs);
        outTwoTIDs.andNot(inAndOutTwoTIDs);
    }

    private void dealCandidateSmall(TrackHauOne track) {
        Integer TID = track.trajectory.TID;
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        DTConstants.supplyCandidate(track, MBR.clone(), passTrackMap, topKTrackMap, segmentIndex, passTrackMap.containsKey(TID));
        Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, passTrackMap, false);
        pruneIndex.alterELem(track, pruneArea);
    }

    /**
     * 轨迹的阈值可能发生变化时，调用该函数，进行修改阈值的一系列操作
     */
    void changeThreshold(TrackHauOne track) {
        double dis = track.getKCanDistance(Constants.topK).distance;
        if (dis > track.threshold){
            //裁剪域变大，topK结果变得不安全了，需要重新计算comparedTrack的topK结果。
            dis = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle pruneArea = DTConstants.enlargePrune(track, dis, segmentIndex, passTrackMap);
            pruneIndex.alterELem(track, pruneArea);
            return;
        }
        dis = track.getKCanDistance(Constants.topK + Constants.t*2).distance;
        if (dis < track.threshold){
            //有更近的topK结果更新裁剪区域即可,为了避免频繁更新，要求threshold的变动超过20
            Rectangle pruneArea = DTConstants.tightenThresholdCommon(track, passTrackMap);
            pruneIndex.alterELem(track, pruneArea);
        }
    }

    /**
     * 处理整条轨迹未完全滑出窗口的轨迹
     */
    private void processUpdatedTrack(Set<TrackHauOne> pruneChangeTracks) {
        RoaringBitmap calculatedTIDs = new RoaringBitmap();
        updateNOPointsTrackHau(pruneChangeTracks, calculatedTIDs, outTwoTIDs.passTIDs, true);
        updateNOPointsTrackHau(pruneChangeTracks, calculatedTIDs, outTwoTIDs.topKTIDs, false);
        updateINPointsTrackHau(pruneChangeTracks, calculatedTIDs, inTwoTIDs.passTIDs, true);
        updateINPointsTrackHau(pruneChangeTracks, calculatedTIDs, inTwoTIDs.topKTIDs, false);
        updateIOPointsTrackHau(pruneChangeTracks, calculatedTIDs, inAndOutTwoTIDs.passTIDs, true);
        updateIOPointsTrackHau(pruneChangeTracks, calculatedTIDs, inAndOutTwoTIDs.topKTIDs, false);
    }

    private void updateNOPointsTrackHau(Set<TrackHauOne> pruneChangeTracks,
                                        RoaringBitmap calculatedTIDs,
                                        RoaringBitmap outTIDs,
                                        boolean isPass) {
        RoaringBitmap comparedInTIDs, comparedOutTIDs, comparedInAndOutTIDs;
        Map<Integer, TrackHauOne> trackMap;
        if (isPass){
            trackMap = passTrackMap;
        }else {
            trackMap = topKTrackMap;
        }
        for (Integer TID : outTIDs) {
            TrackHauOne track = trackMap.get(TID);
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTID = state.getStateAnoTID(TID);
                if (!calculatedTIDs.contains(comparedTID)) { //track与comparedTid的距离没有计算过
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
                    List<TrackPoint> comparedInPoints;
                    if (comparedTrack == null){
                        comparedTrack = topKTrackMap.get(comparedTID);
                        comparedInTIDs = inTwoTIDs.topKTIDs;
                        comparedOutTIDs = outTwoTIDs.topKTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.topKTIDs;
                        comparedInPoints = addTopKPoints.get(comparedTID);
                    }else {
                        comparedInTIDs = inTwoTIDs.passTIDs;
                        comparedOutTIDs = outTwoTIDs.passTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.passTIDs;
                        comparedInPoints = addPassPoints.get(comparedTID);
                    }
                    if (comparedInAndOutTIDs.contains(comparedTID)) {
                        Hausdorff.NOIOHausdorff(track.trajectory, comparedTrack.trajectory, comparedInPoints, state);
                    }else if (comparedOutTIDs.contains(comparedTID)){
                        Hausdorff.NONOHausdorff(track.trajectory, comparedTrack.trajectory, state);
                    }else if (comparedInTIDs.contains(comparedTID)){
                        Hausdorff.NOINHausdorff(track.trajectory, comparedTrack.trajectory, comparedInPoints, state);
                    }else {
                        Hausdorff.NONNHausdorff(track.trajectory, comparedTrack.trajectory, state);
                        if (isPass) {
                            int oldIndex = comparedTrack.candidateInfo.indexOf(TID);
                            if (oldIndex != -1) {
                                comparedTrack.updateCandidateInfo(TID);
                                pruneChangeTracks.add(comparedTrack);
                            }
                        }
                    }
                }
            }
            track.sortCandidateInfo();
            recalculateTrack(track, track.getPruningRegion(0.0), isPass);
            calculatedTIDs.add(TID);
        }
    }

    private void updateINPointsTrackHau(Set<TrackHauOne> pruneChangeTracks,
                                        RoaringBitmap calculatedTIDs,
                                        RoaringBitmap inTIDs,
                                        boolean isPass) {
        RoaringBitmap comparedInTIDs, comparedOutTIDs, comparedInAndOutTIDs;
        Map<Integer, TrackHauOne> trackMap;
        Map<Integer, List<TrackPoint>> inPointsMap;
        if (isPass){
            trackMap = passTrackMap;
            inPointsMap = addPassPoints;
        }else {
            trackMap = topKTrackMap;
            inPointsMap = addTopKPoints;
        }
        for (Integer TID : inTIDs) {
            TrackHauOne track = trackMap.get(TID);
            List<TrackPoint> inPoints = inPointsMap.get(TID);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
            for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                SimilarState state = ite.next().getValue();
                int comparedTID = state.getStateAnoTID(TID);
                if (!calculatedTIDs.contains(comparedTID)) { //track与comparedTid的距离没有计算过
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
                    List<TrackPoint> comparedInPoints;
                    if (comparedTrack == null){
                        comparedTrack = topKTrackMap.get(comparedTID);
                        comparedInTIDs = inTwoTIDs.topKTIDs;
                        comparedOutTIDs = outTwoTIDs.topKTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.topKTIDs;
                        comparedInPoints = addTopKPoints.get(comparedTID);
                    }else {
                        comparedInTIDs = inTwoTIDs.passTIDs;
                        comparedOutTIDs = outTwoTIDs.passTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.passTIDs;
                        comparedInPoints = addPassPoints.get(comparedTID);
                    }
                    if (comparedInAndOutTIDs.contains(comparedTID)) {
                        Hausdorff.INIOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, comparedInPoints, state);
                    } else if (comparedOutTIDs.contains(comparedTID)) {
                        Hausdorff.INNOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                    } else if (comparedInTIDs.contains(comparedTID)) {
                        Hausdorff.ININHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, comparedInPoints, state);
                    } else {
                        Hausdorff.INNNHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                        if (isPass && addPruneChange(TID, comparedTrack, pointsMBR, pruneChangeTracks)){
                            if (!track.candidateInfo.contains(comparedTID)) {
                                comparedTrack.getRelatedInfo().remove(state);
                                ite.remove();
                            }
                        }
                    }
                }
            }
            track.sortCandidateInfo();
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold).getUnionRectangle(pointsMBR);
            recalculateTrack(track, MBR, isPass);
            calculatedTIDs.add(TID);
        }
    }

    private void updateIOPointsTrackHau(Set<TrackHauOne> pruneChangeTracks,
                                        RoaringBitmap calculatedTIDs,
                                        RoaringBitmap inAndOutTIDs,
                                        boolean isPass) {
        RoaringBitmap comparedInTIDs, comparedOutTIDs, comparedInAndOutTIDs;
        Map<Integer, TrackHauOne> trackMap;
        Map<Integer, List<TrackPoint>> inPointsMap;
        if (isPass){
            trackMap = passTrackMap;
            inPointsMap = addPassPoints;
        }else {
            trackMap = topKTrackMap;
            inPointsMap = addTopKPoints;
        }
        for (Integer TID : inAndOutTIDs) {
            TrackHauOne track = trackMap.get(TID);
            List<TrackPoint> inPoints = inPointsMap.get(TID);
            Rectangle pointsMBR = null;
            if (isPass){
                pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
                pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
            }
            for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                SimilarState state = ite.next().getValue();
                int comparedTID = state.getStateAnoTID(TID);
                if (!calculatedTIDs.contains(comparedTID)) { //track与comparedTid的距离没有计算过
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
                    List<TrackPoint> comparedInPoints;
                    if (comparedTrack == null){
                        comparedTrack = topKTrackMap.get(comparedTID);
                        comparedInTIDs = inTwoTIDs.topKTIDs;
                        comparedOutTIDs = outTwoTIDs.topKTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.topKTIDs;
                        comparedInPoints = addTopKPoints.get(comparedTID);
                    }else {
                        comparedInTIDs = inTwoTIDs.passTIDs;
                        comparedOutTIDs = outTwoTIDs.passTIDs;
                        comparedInAndOutTIDs = inAndOutTwoTIDs.passTIDs;
                        comparedInPoints = addPassPoints.get(comparedTID);
                    }
                    if (comparedInAndOutTIDs.contains(comparedTID)) {
                        Hausdorff.IOIOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, comparedInPoints, state);
                    } else if (comparedOutTIDs.contains(comparedTID)) {
                        Hausdorff.IONOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                    } else if (comparedInTIDs.contains(comparedTID)) {
                        Hausdorff.IOINHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, comparedInPoints, state);
                    } else {
                        Hausdorff.IONNHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                        if (isPass && addPruneChange(TID, comparedTrack, pointsMBR, pruneChangeTracks)){
                            if (!track.candidateInfo.contains(comparedTID)) {
                                comparedTrack.getRelatedInfo().remove(state);
                                ite.remove();
                            }
                        }
                    }
                }
            }
            track.sortCandidateInfo();
            recalculateTrack(track, track.getPruningRegion(0.0), isPass);
            calculatedTIDs.add(TID);
        }
    }

    private void recalculateTrack(TrackHauOne track, Rectangle MBR, boolean isPass) {
        DTConstants.supplyCandidate(track, MBR, passTrackMap, topKTrackMap, segmentIndex,isPass);
        Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, passTrackMap, true);
        pruneIndex.alterELem(track, pruneArea);
    }

    private boolean addPruneChange(Integer TID,
                                   TrackHauOne comparedTrack,
                                   Rectangle pointsMBR,
                                   Set<TrackHauOne> pruneChangeTracks) {
        int oldIndex = comparedTrack.candidateInfo.indexOf(TID);
        if (oldIndex != -1) {
            if (comparedTrack.rect.isInternal(pointsMBR)){
                comparedTrack.updateCandidateInfo(TID);
                pruneChangeTracks.add(comparedTrack);
            }else {
                comparedTrack.candidateInfo.remove(oldIndex);
                pruneChangeTracks.add(comparedTrack);
                return true;
            }
        }
        return false;
    }



    /**
     * 处理完全移除的轨迹
     */
    private void dealAllSlideOutTracks(Set<TrackHauOne> pruneChangeTracks,
                                       Set<Integer> passEmptyTIDs,
                                       Set<Integer> topKEmptyTIDs) {
        for (Integer TID : passEmptyTIDs) {
            outTwoTIDs.passTIDs.remove(TID);
            allAlterTIDs.remove(TID);
        }
        for (Integer TID : topKEmptyTIDs) {
            outTwoTIDs.topKTIDs.remove(TID);
            allAlterTIDs.remove(TID);
        }
        for (Integer TID : passEmptyTIDs) {
            TrackHauOne track = passTrackMap.get(TID);
            pruneIndex.delete(track);
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = state.getStateAnoTID(TID);
                if (passEmptyTIDs.contains(comparedTid) || topKEmptyTIDs.contains(comparedTid))
                    continue;
                TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
                if (comparedTrack == null) comparedTrack = topKTrackMap.get(comparedTid);
                int index = comparedTrack.candidateInfo.indexOf(TID);
                comparedTrack.removeRelatedInfo(state);
                if (index != -1) {
                    comparedTrack.candidateInfo.remove(index);
                    if (!allAlterTIDs.contains(comparedTid))
                        pruneChangeTracks.add(comparedTrack);
                }
            }
        }
        for (Integer TID : topKEmptyTIDs) {
            TrackHauOne track = topKTrackMap.get(TID);
            pruneIndex.delete(track);
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = state.getStateAnoTID(TID);
                if (passEmptyTIDs.contains(comparedTid) || topKEmptyTIDs.contains(comparedTid))
                    continue;
                TrackHauOne  comparedTrack = passTrackMap.get(comparedTid);
                comparedTrack.removeRelatedInfo(state);
            }
        }
        for (Integer TID : passEmptyTIDs) passTrackMap.remove(TID);
        for (Integer TID : topKEmptyTIDs) topKTrackMap.remove(TID);
    }

    private void preElements(Set<TrackHauOne> pruneChangeTracks) {
        TwoTIDs inTwoTIDs = new TwoTIDs();
        tIDsMap.put(winStart, inTwoTIDs);
        //0:  添加经过点
        for (Map.Entry<Integer, List<TrackPoint>> entry : addPassPoints.entrySet()) {
            TrackHauOne track = passTrackMap.get(entry.getKey());
            inTwoTIDs.passTIDs.add(entry.getKey());
            List<Segment> segments = track.trajectory.addTrackPoints(entry.getValue());
            segments.forEach(segment -> segmentIndex.insert(segment));
        }
        //1:  添加topK点
        for (Map.Entry<Integer, List<TrackPoint>> entry : addTopKPoints.entrySet()) {
            TrackHauOne track = topKTrackMap.get(entry.getKey());
            inTwoTIDs.topKTIDs.add(entry.getKey());
            track.trajectory.addTrackPoints(entry.getValue());
        }
        //2： 新增经过轨迹
        addPassTracks.forEach(this::addPassTrack);
        //3： 新增topK轨迹
        addTopKTracks.forEach(this::addTopKTrack);
        //5： 删除topK轨迹
        delTopKTIDs.forEach(this::removeTopKTrack);
        //4： 删除经过轨迹
        delPassTIDs.forEach(TID -> removePassTrack(pruneChangeTracks, TID));
        //6.  经过轨迹改为topK轨迹
        convertPassTIDs.forEach(TID -> convertPassTrack(pruneChangeTracks, TID));
        //7.  topK轨迹改为经过轨迹
        convertTopKTIDs.forEach(this::convertTopKTrack);
    }

    private void addPassTrack(G2LPoints glPoints) {
        if (passTrackMap.containsKey(glPoints.TID) || topKTrackMap.containsKey(glPoints.TID))
            throw new IllegalArgumentException();
        TrackHauOne track = glPoints.toTrackHauOne();
        for (Segment segment : track.trajectory.elms) {
            Long mapKey = ((segment.getSecondTime()/Constants.windowSize)) * Constants.windowSize;
            tIDsMap.computeIfAbsent(mapKey, aLong -> new TwoTIDs()).passTIDs.add(glPoints.TID);
            segmentIndex.insert(segment);
        }
        passTrackMap.put(glPoints.TID, track);
    }

    private void addTopKTrack(G2LPoints glPoints) {
        if (passTrackMap.containsKey(glPoints.TID) || topKTrackMap.containsKey(glPoints.TID))
            throw new IllegalArgumentException();
        TrackHauOne track = glPoints.toTrackHauOne();
        for (Segment segment : track.trajectory.elms) {
            Long mapKey = ((segment.getSecondTime()/ Constants.windowSize)) * Constants.windowSize;
            tIDsMap.computeIfAbsent(mapKey, aLong -> new TwoTIDs()).topKTIDs.add(segment.getTID());
        }
        topKTrackMap.put(glPoints.TID, track);
    }

    private void removeTopKTrack(Integer TID) {
        TrackHauOne track = topKTrackMap.remove(TID);
        pruneIndex.delete(track);
        for (SimilarState state : track.getRelatedInfo().values()) {
            Integer comparedTid = state.getStateAnoTID(TID);
            passTrackMap.get(comparedTid).removeRelatedInfo(state);
        }
        for (Segment segment : track.trajectory.elms) {
            Long mapKey = ((segment.getSecondTime()/ Constants.windowSize)) * Constants.windowSize;
            tIDsMap.get(mapKey).topKTIDs.remove(TID);
        }
    }

    private void removePassTrack(Set<TrackHauOne> pruneChangeTracks, Integer TID) {
        TrackHauOne track = passTrackMap.remove(TID);
        pruneChangeTracks.remove(track);
        pruneIndex.delete(track);
        for (Segment segment : track.trajectory.elms) {
            segmentIndex.delete(segment);
            Long mapKey = ((segment.getSecondTime()/ Constants.windowSize)) * Constants.windowSize;
            tIDsMap.get(mapKey).passTIDs.remove(TID);
        }
        for (SimilarState state : track.getRelatedInfo().values()) {
            Integer comparedTid = state.getStateAnoTID(TID);
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            if (comparedTrack != null){
                int index = comparedTrack.candidateInfo.indexOf(TID);
                comparedTrack.removeRelatedInfo(state);
                if (index != -1) {
                    comparedTrack.candidateInfo.remove(index);
                    pruneChangeTracks.add(comparedTrack);
                }
            }
        }
    }

    private void convertPassTrack(Set<TrackHauOne> pruneChangeTracks, Integer TID) {
        TrackHauOne track = passTrackMap.remove(TID);
        topKTrackMap.put(TID, track);
        for (Segment segment : track.trajectory.elms) {
            segmentIndex.delete(segment);
            Long mapKey = ((segment.getSecondTime()/ Constants.windowSize)) * Constants.windowSize;
            TwoTIDs twoTIDs = tIDsMap.get(mapKey);
            twoTIDs.passTIDs.remove(TID);
            twoTIDs.topKTIDs.add(TID);
        }
        List<TrackPoint> points = addPassPoints.remove(TID);
        if (points != null) addTopKPoints.put(TID, points);
        Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator();
        for (;ite.hasNext();) {
            SimilarState state = ite.next().getKey();
            Integer comparedTid = state.getStateAnoTID(TID);
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            if (!track.candidateInfo.contains(comparedTid)){
                comparedTrack.removeRelatedInfo(state);
                if (comparedTrack.candidateInfo.remove(TID))
                    pruneChangeTracks.add(comparedTrack);
                else
                    throw new IllegalArgumentException("");
                ite.remove();
            }
            if (comparedTrack.candidateInfo.remove(TID))
                pruneChangeTracks.add(comparedTrack);
        }
    }

    private void convertTopKTrack(Integer TID) {
        TrackHauOne track = topKTrackMap.remove(TID);
        passTrackMap.put(TID, track);
        for (Segment segment : track.trajectory.elms) {
            segmentIndex.insert(segment);
            Long mapKey = ((segment.getSecondTime()/ Constants.windowSize)) * Constants.windowSize;
            TwoTIDs twoTIDs = tIDsMap.get(mapKey);
            twoTIDs.topKTIDs.remove(TID);
            twoTIDs.passTIDs.add(TID);
        }
        List<TrackPoint> points = addTopKPoints.remove(TID);
        if (points != null) addPassPoints.put(TID, points);
    }

    private void startUseSubTask() {
        hasInit = true;
        passTrackMap = new HashMap<>();
        topKTrackMap = new HashMap<>();
        Rectangle region = (Rectangle)newRegion.value;
        segmentIndex = new LocalTree<>(4,1,11, region, true);
        pruneIndex  = new LocalTree<>(4,1,11, region, false);
    }

    private void classifyElements(Iterable<G2LElem> elements) {
        G2LPoints points;
        for (G2LElem element : elements) {
            switch (element.flag){
                case 0:
                    points = (G2LPoints) element.value;
                    addPassPoints.put(points.TID, points.points);
                    break;
                case 1:
                    points = (G2LPoints) element.value;
                    addTopKPoints.put(points.TID, points.points);
                    break;
                case 2:
                    addPassTracks.add((G2LPoints) element.value);
                    break;
                case 3:
                    addTopKTracks.add((G2LPoints) element.value);
                    break;
                case 4:
                    delPassTIDs.add(((G2TID) element.value).TID);
                    break;
                case 5:
                    delTopKTIDs.add(((G2TID) element.value).TID);
                    break;
                case 6:
                    convertPassTIDs.add(((G2TID) element.value).TID);
                    break;
                case 7:
                    convertTopKTIDs.add(((G2TID) element.value).TID);
                    break;
                case 14:
                    newRegion = element;
                    break;
                case 15:
                    verifyPass.add((G2LPoints) element.value);
                    break;
                case 16:
                    verifyTopK.add((G2LPoints) element.value);
                    break;
                case 17:
                    count = ((G2LCount) element.value).count;
                    break;
                default:
                    adjustInfo.add(element);
                    break;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        tIDsMap = new HashMap<>();
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        hasInit = false;
        addPassPoints = new HashMap<>();
        addTopKPoints = new HashMap<>();
        addPassTracks = new ArrayList<>();
        addTopKTracks = new ArrayList<>();
        delPassTIDs = new HashSet<>();
        delTopKTIDs = new HashSet<>();
        convertPassTIDs = new ArrayList<>();
        convertTopKTIDs = new ArrayList<>();
        adjustInfo = new ArrayList<>();
        verifyPass = new ArrayList<>();
        verifyTopK = new ArrayList<>();
    }

    @Getter
    @Setter
    static class TwoTIDs implements Cloneable{
        RoaringBitmap passTIDs;
        RoaringBitmap topKTIDs;

        public TwoTIDs() {
            this.passTIDs = new RoaringBitmap();
            this.topKTIDs = new RoaringBitmap();
        }

        public TwoTIDs(RoaringBitmap passTIDs, RoaringBitmap topKTIDs) {
            this.passTIDs = passTIDs;
            this.topKTIDs = topKTIDs;
        }

        @Override
        protected TwoTIDs clone() throws CloneNotSupportedException {
            TwoTIDs twoTIDs = (TwoTIDs) super.clone();
            twoTIDs.topKTIDs = topKTIDs.clone();
            twoTIDs.passTIDs = passTIDs.clone();
            return twoTIDs;
        }

        public void and(TwoTIDs twoTIDs) {
            passTIDs.and(twoTIDs.passTIDs);
            topKTIDs.and(twoTIDs.topKTIDs);
        }

        public void andNot(TwoTIDs twoTIDs) {
            passTIDs.andNot(twoTIDs.passTIDs);
            topKTIDs.andNot(twoTIDs.topKTIDs);
        }

        public void clear() {
            passTIDs.clear();
            topKTIDs.clear();
        }
    }

}
