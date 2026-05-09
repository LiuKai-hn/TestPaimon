package com.qcc.source;
import com.qcc.pojo.QccStandardEntity;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;
import java.util.UUID;

/*
 *
 * @author liukai
 * @date 2026/5/9 11:56
 */

/**
 * 自定义Source：模拟企查查 工商/风险/舆情 多源数据
 * 本地可运行，无第三方依赖
 */
public class QccMockSource implements SourceFunction<QccStandardEntity> {
    private final int sourceType;
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    // 1=工商 2=风险 3=舆情
    public QccMockSource(int sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public void run(SourceContext<QccStandardEntity> ctx) throws Exception {
        while (isRunning) {
            QccStandardEntity entity = new QccStandardEntity();
            entity.setId(UUID.randomUUID().toString());
            entity.setCompanyCode("91310000" + (100000 + random.nextInt(900000)));
            entity.setCompanyName("上海科技公司_" + random.nextInt(100));
            entity.setDataType(sourceType);

            // 模拟业务数据
            if (sourceType == 1) entity.setContent("工商信息：法人张三，注册资本1000W");
            else if (sourceType == 2) entity.setContent("风险信息：行政处罚1次");
            else entity.setContent("舆情信息：中标项目100W");

            entity.setTs(System.currentTimeMillis());
            ctx.collect(entity);

            // 本地限流，不产生压力
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
