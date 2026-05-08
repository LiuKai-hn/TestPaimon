package com.liukai.func;


/*
 *
 * @author liukai
 * @date 2026/4/23 14:43
 */

import com.liukai.apps.MultiDimMerge_Production;
import com.liukai.pojo.UniformMessage;
import com.liukai.pojo.UserFull;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// =========================================
// 核心：状态合并 + TTL
// =========================================
public class UserMergeProcessFunction extends KeyedProcessFunction<Integer, UniformMessage, UserFull> {

    private ValueState<UserFull> fullState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<UserFull> desc = new ValueStateDescriptor<>("userFullState", UserFull.class);

        // ========== TTL：365 天未更新自动清理 ==========
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(365))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        desc.enableTimeToLive(ttlConfig);

        fullState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(UniformMessage msg,
                               Context ctx,
                               Collector<UserFull> out) throws Exception {

        UserFull curr = fullState.value();
        if (curr == null) {
            curr = new UserFull(msg.getUserId(), msg.getName(), msg.getLevel(), msg.getCity(), null);
        }

        curr.setUpdateTime(new java.sql.Timestamp(System.currentTimeMillis()));
        fullState.update(curr);
        out.collect(curr);
    }
}
