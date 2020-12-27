package com.ada.QBSTree;

import com.ada.geometry.Point;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@SuppressWarnings("rawtypes")
@Getter
@Setter
public class ElemRoot extends Point implements Cloneable ,Serializable {
    transient public RCDataNode leaf;

    public ElemRoot() { }

    public ElemRoot(RCDataNode leaf, double[] data) {
        super(data);
        this.leaf = leaf;
    }

    public boolean check(){
        if (!leaf.elms.contains(this))
            return false;
        RCNode node = leaf;
        while (!node.isRoot()){
            if (node.parent.child[node.position] != node)
                return false;
            node = node.parent;
        }
        return leaf.tree.root == node;
    }

    @Override
    public ElemRoot clone() {
        ElemRoot elemRoot = (ElemRoot) super.clone();
        elemRoot.leaf = null;
        return elemRoot;
    }

}
