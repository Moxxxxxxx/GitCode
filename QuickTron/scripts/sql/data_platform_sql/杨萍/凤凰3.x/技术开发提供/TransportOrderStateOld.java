package com.kc.phoenix.rss.order.enums;

import com.google.common.collect.Lists;
import com.kc.phoenix.common.data.spec.RobotJobState;
import com.kc.phoenix.common.utils.StreamUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 作业单状态（旧的，不要使用了）
 *
 * @author zhongjing
 */
@Deprecated
public enum TransportOrderStateOld implements RobotJobState {
    /**
     * 未执行
     */
    INIT(0, "INIT", "未执行"),
    /**
     * 待执行
     */
    WAITING_NEXTSTOP(10, "WAITING_NEXTSTOP", "待执行"),
    /**
     * 待分配ROBOT
     */
    WAITING_ROBOT(20, "WAITING_ROBOT", "待分配ROBOT"),
    /**
     * 待下发
     */
    WAITING_RESOURCE(22, "WAITING_RESOURCE", "待下发"),
    /**
     * 待下发(多楼层)
     */
    PENDING_INIT_JOB(23, "PENDING_INIT_JOB", "待下发(多楼层)"),
    /**
     * 等待任务下发
     */
    WAITING_DISPATCHER(25, "WAITING_DISPACHER", "等待任务下发"),
    /**
     * 已分配
     */
    INIT_JOB(30, "INIT_JOB", "已分配"),
    /**
     * 执行中
     */
    EXECUTING(35, "EXECUTING", "执行中"),
    /**
     * 开始空车移动（潜伏式）
     */
    MOVE_START(36, "MOVE_START", "开始空车移动"),
    /**
     * 空车移动结束（潜伏式）
     */
    MOVE_DONE(37, "MOVE_DONE", "空车移动结束"),
    /**
     * 开始顶升
     */
    LIFT_UP_START(40, "LIFT_UP_START", "开始顶升"),
    /**
     * 顶升完成
     */
    LIFT_UP_DONE(45, "LIFT_UP_DONE", "顶升完成"),
    /**
     * 开始移动
     */
    MOVE_BEGIN(50, "MOVE_BEGIN", "开始移动"),
    /**
     * 带载开始移动(潜伏式)
     */
    RACK_MOVE_START(55, "RACK_MOVE_START", "带载开始移动"),
    /**
     * 带载开始结束(潜伏式)
     */
    RACK_MOVE_DONE(56, "RACK_MOVE_DONE", "带载开始结束"),
    /**
     * 急停
     */
    SUSPEND(60, "SUSPEND", "急停"),
    /**
     * 开始放下
     */
    PUT_DOWN_START(70, "PUT_DOWN_START", "开始放下货架"),
    /**
     * 对于二段位移，会将其同步到robot_job
     */
    PUT_DOWN_DONE(75, "PUT_DOWN_DONE", "放下货架完成"),
    /**
     * 开始放下
     */
    START_PUT_DOWN(70, "START_PUT_DOWN", "开始放下"),
    /**
     * 放下完成
     */
    DONE_PUT_DOWN(75, "DONE_PUT_DOWN", "放下完成"),
    /**
     * 到站
     */
    ENTER_STATION(80, "ENTER_STATION", "到站"),

    /**
     * 开始空车移动（叉式）
     */
    FORK_MOVE_START(38, "FORK_MOVE_START", "空车移动"),
    /**
     * 空车移动完成（叉式）
     */
    FORK_MOVE_DONE(39, "FORK_MOVE_DONE", "空车移动完成"),
    /**
     * 叉取开始（叉式）
     */
    PALLET_FORK_UP_START(41, "PALLET_FORK_UP_START", "叉取开始"),
    /**
     * 叉取完成（叉式）
     */
    PALLET_FORK_UP_DONE(42, "PALLET_FORK_UP_DONE", "叉取完成"),
    /**
     * 带载移动开始（叉式）
     */
    PALLET_MOVE_START(44, "PALLET_MOVE_START", "带载移动开始"),
    /**
     * 带载移动完成（叉式）
     */
    PALLET_MOVE_DONE(45, "PALLET_MOVE_DONE", "带载移动完成"),
    /**
     * 放下开始（叉式）
     */
    PALLET_PUT_DOWN_START(47, "PALLET_FORK_UP_START", "放下开始"),
    /**
     * 放下完成（叉式）
     */
    PALLET_PUT_DOWN_DONE(48, "PALLET_FORK_UP_DONE", "放下完成"),
    /**
     * 开始二次移动
     */
    AGAIN_MOVE_START(84, "AGAIN_MOVE_START", "开始二次移动"),
    /**
     * 二次移动完成
     */
    AGAIN_MOVE_DONE(85, "AGAIN_MOVE_DONE", "二次移动完成"),
    /**
     * 完成
     */
    DONE(90, "DONE", "完成"),
    /**
     * 挂起
     */
    PENDING(100, "PENDING", "挂起"),
    /**
     * 取消中
     */
    CANCEL_EXECUTING(105, "CANCEL_EXECUTING", "取消中"),
    /**
     * 取消
     */
    CANCEL(110, "CANCEL", "取消"),
    /**
     * 失败
     */
    FAILED(120, "FAILED", "失败"),
    /**
     * 异常完成
     */
    ABNORMAL_COMPLETED(130, "ABNORMAL_COMPLETED", "异常完成"),

    /**
     * 等待搬运阻挡货架
     */
    WAITING_HDS(138, "WAITING_HDS", "等待搬运阻挡货架"),

    /**
     * 触发阻塞货架搬运
     */
    DOING_HDS(139, "DOING_HDS", "触发阻塞货架搬运"),

    /**
     * 等待分配电梯
     */
    WAITING_LIFT(140, "WAITING_LIFT", "等待分配电梯"),
    /**
     * 异常取消
     */
    ABNORMAL_CANCEL(150, "ABNORMAL_CANCEL", "异常取消"),
    /**
     * 上料完成
     */
    LOAD_COMPLETED(155, "LOAD_COMPLETED", "上料完成"),
    /**
     * 下料完成
     */
    UNLOAD_COMPLETED(156, "UNLOAD_COMPLETED", "下料完成"),
    /**
     * 上料执行中
     */
    LOAD_EXECUTING(160, "LOAD_EXECUTING", "上料执行中"),
    /**
     * 辊筒小车空车移位完成，开始接料
     */
    PROCESS(170, "PROCESS", "开始接料"),
    /**
     * 辊筒小车空满交换逻辑，延伸预占
     */
    EXTEND_OCCUPY(171, "EXTEND_OCCUPY", "延伸预占"),

    /**
     * 对于上游控制的密集存储任务，如果当前货架发现会阻挡其它货架返库， 那么让robot扛着当前货架游荡
     */
    WAITING_STRAY(180, "WAITING_STRAY", "返库流浪"),
    /**
     * 等待投递
     */
    PENDING_DELIVER(181, "PENDING_DELIVER", "等待投递"),
    /**
     * 到达投递点
     */
    ARRIVE_TARGET(182, "ARRIVE_TARGET", "到达投递点"),
    /**
     * 投递中
     */
    DELIVER_EXECUTING(185, "DELIVER_EXECUTING", "投递中"),
    /**
     * 投递完成
     */
    DELIVER_DONE(190, "DELIVER_DONE", "投递完成");

    private int code;
    private String enDesc;
    private String desc;

    TransportOrderStateOld(int code, String enDesc, String desc) {
        this.code = code;
        this.enDesc = enDesc;
        this.desc = desc;

    }

    public String getDesc() {
        return this.desc;
    }

    public String getEnDesc() {
        return this.enDesc;
    }

    public int getCode() {
        return this.code;
    }

    public static List<TransportOrderStateOld> getAllStates() {
        return ALL_STATES;
    }

    /**
     * 获取电梯能感知的状态， 需要跨楼层的任务，当第一段robot任务下发rcs后robotJob变成INIT_JOB，
     * 直到该搬运任务结束前会一直停留在这个状态，电梯的状态不同步回robotJob
     *
     * @return
     */
    public static List<String> getLiftAwareState() {
        /**
         * 对于电梯，
         */
        return Arrays.asList(WAITING_ROBOT, WAITING_LIFT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START,
                        LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, MOVE_BEGIN, PUT_DOWN_START, PUT_DOWN_DONE, AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<String> workingState() {
        return Arrays.asList(INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, WAITING_DISPATCHER, MOVE_BEGIN, ENTER_STATION,
                        LOAD_COMPLETED, AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<String> getNotScheduledState() {
        return Arrays.asList(WAITING_NEXTSTOP, WAITING_ROBOT, INIT, WAITING_RESOURCE, PENDING_INIT_JOB,
                WAITING_DISPATCHER).stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<String> getScheduledState() {
        return Arrays.asList(INIT_JOB, EXECUTING, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, MOVE_BEGIN, SUSPEND, PUT_DOWN_START, PUT_DOWN_DONE,
                        ENTER_STATION, DONE, PENDING, CANCEL, FAILED, ABNORMAL_COMPLETED, WAITING_LIFT,
                        ABNORMAL_CANCEL, LOAD_COMPLETED, LOAD_EXECUTING, PROCESS, PENDING_DELIVER,
                        DELIVER_EXECUTING, DELIVER_DONE, AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<TransportOrderStateOld> getMockStationEffectiveState() {
        return Arrays.asList(WAITING_NEXTSTOP, WAITING_ROBOT, WAITING_HDS, DOING_HDS, INIT_JOB, MOVE_START, MOVE_DONE,
                LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START, RACK_MOVE_DONE, MOVE_BEGIN, ENTER_STATION,
                AGAIN_MOVE_START, AGAIN_MOVE_DONE);
    }

    /**
     * 未完成状态
     */
    public static List<TransportOrderStateOld> getNotDoneStates() {
        return Arrays.stream(values()).collect(Collectors.toList()).stream().filter(state ->
                !getDoneStates().contains(state.name())).collect(Collectors.toList());
    }

    public static List<String> getDoneStates() {
        List<String> states = Lists.newArrayList(
                DONE,
                ENTER_STATION,
                CANCEL,
                ABNORMAL_COMPLETED,
                ABNORMAL_CANCEL).stream().map(Enum::name).collect(Collectors.toList());
        return states;
    }

    public static List<String> getDoneStatesV2() {
        List<String> states = Lists.newArrayList(
                DONE,
                CANCEL,
                FAILED,
                ABNORMAL_COMPLETED,
                ABNORMAL_CANCEL).stream().map(Enum::name).collect(Collectors.toList());
        return states;
    }

    public static List<String> getAbnormalStates() {
        List<String> states = Lists.newArrayList(
                CANCEL,
                ABNORMAL_COMPLETED,
                ABNORMAL_CANCEL).stream().map(Enum::name).collect(Collectors.toList());
        return states;
    }

    public static List<String> occupyPointState() {
        List<String> states = Lists.newArrayList(
                WAITING_ROBOT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                RACK_MOVE_DONE,
                MOVE_BEGIN, PUT_DOWN_START, PUT_DOWN_DONE, ENTER_STATION).stream().map(Enum::name).collect(Collectors.toList());
        return states;
    }

    /**
     * 可以取消的任务状态
     */
    public static List<TransportOrderStateOld> getCancelAbleStates() {

        return Lists.newArrayList(INIT, WAITING_NEXTSTOP, WAITING_ROBOT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START,
                LIFT_UP_DONE, SUSPEND, RACK_MOVE_START, RACK_MOVE_DONE, MOVE_BEGIN, PUT_DOWN_START, PUT_DOWN_DONE,
                ENTER_STATION, PENDING);

    }

    /**
     * 不可以急停的任务状态
     */
    public static List<String> getNotSuspendStates() {
        return Lists.newArrayList(CANCEL.name(), ENTER_STATION.name(), SUSPEND.name(),
                DONE.name(), FAILED.name(), ABNORMAL_COMPLETED.name(), ABNORMAL_CANCEL.name(), PENDING.name());
    }

    /**
     * 已完结状态
     */
    public static List<String> getCompletedStates() {
        return Lists.newArrayList(CANCEL.name(),
                DONE.name(),
                FAILED.name(),
                ABNORMAL_COMPLETED.name(),
                ABNORMAL_CANCEL.name());
    }

    public static List<String> getWorkingStates() {
        return Lists.newArrayList(
                WAITING_HDS.name(), DOING_HDS.name(), WAITING_ROBOT.name(), WAITING_LIFT.name(),
                INIT_JOB.name(), MOVE_START.name(), MOVE_DONE.name(), LIFT_UP_START.name(), LIFT_UP_DONE.name(),
                MOVE_BEGIN.name(),
                RACK_MOVE_START.name(), RACK_MOVE_DONE.name(), PUT_DOWN_START.name(), PUT_DOWN_DONE.name(),
                ENTER_STATION.name());
    }

    /**
     * 获取hds 未完成任务的状态
     */
    public static List<String> getHdsAwareStates() {
        return Lists.newArrayList(
                WAITING_HDS.name(),
                MOVE_BEGIN.name(),
                ENTER_STATION.name()
        );
    }

    /**
     * 可以被取消的堆高车任务状态
     *
     * @return
     */
    public static List<TransportOrderStateOld> getForkCancelStates() {
        return Lists.newArrayList(WAITING_ROBOT, WAITING_NEXTSTOP, INIT_JOB, PENDING);
    }

    /**
     * 可以被取消的跨楼层任务状态
     *
     * @return
     */
    public static List<TransportOrderStateOld> getAcrossFloorCancelStates() {
        return Lists.newArrayList(WAITING_NEXTSTOP, WAITING_LIFT, WAITING_ROBOT, INIT_JOB,
                LIFT_UP_DONE, MOVE_BEGIN, PENDING);
    }

    /**
     * 可以被取消的翻版车任务状态
     *
     * @return
     */
    public static List<TransportOrderStateOld> getReprintCancelStates() {
        return Lists.newArrayList(WAITING_ROBOT, WAITING_NEXTSTOP, PENDING);
    }

    /**
     * 未完成的小皮带投递任务
     *
     * @return
     */
    public static List<String> getNotDoneDeliverState() {
        return Lists.newArrayList(WAITING_ROBOT.name(), PENDING_DELIVER.name(), INIT.name(), EXECUTING.name());
    }

    /**
     * 待执行的任务
     *
     * @return
     */
    public static List<TransportOrderStateOld> getPendingStates() {
        return Lists.newArrayList(WAITING_NEXTSTOP, INIT);
    }

    @Override
    public boolean isFinal() {
        return this == ENTER_STATION || this == DONE;
    }

    private static final List<TransportOrderStateOld> ALL_STATES;
    private static final List<String> CONSOLE_DONE_STATE;
    private static final List<String> CONSOLE_EXCEPTION_STATE;
    private static final List<String> CONSOLE_EXECUTING_STATE;
    private static final List<String> CONSOLE_WAITING_STATE;

    private static List<String> toArray(TransportOrderStateOld... states) {
        return StreamUtils.mapToList(Arrays.asList(states), TransportOrderStateOld::name);
    }

    static {
        ALL_STATES = Arrays.asList(values());
        CONSOLE_DONE_STATE = toArray(TransportOrderStateOld.DONE, TransportOrderStateOld.ABNORMAL_COMPLETED);
        CONSOLE_WAITING_STATE = toArray(
                TransportOrderStateOld.WAITING_NEXTSTOP,
                TransportOrderStateOld.WAITING_ROBOT,
                TransportOrderStateOld.INIT,
                TransportOrderStateOld.WAITING_RESOURCE,
                TransportOrderStateOld.PENDING_INIT_JOB,
                TransportOrderStateOld.WAITING_DISPATCHER,
                TransportOrderStateOld.WAITING_LIFT);
        CONSOLE_EXCEPTION_STATE = toArray(
                TransportOrderStateOld.SUSPEND,
                TransportOrderStateOld.PENDING,
                TransportOrderStateOld.CANCEL,
                TransportOrderStateOld.FAILED,
                TransportOrderStateOld.ABNORMAL_CANCEL);
        CONSOLE_EXECUTING_STATE = toArray(
                TransportOrderStateOld.INIT_JOB,
                TransportOrderStateOld.EXECUTING,
                TransportOrderStateOld.MOVE_START,
                TransportOrderStateOld.MOVE_DONE,
                TransportOrderStateOld.LIFT_UP_START,
                TransportOrderStateOld.LIFT_UP_DONE,
                TransportOrderStateOld.MOVE_BEGIN,
                TransportOrderStateOld.RACK_MOVE_START,
                TransportOrderStateOld.RACK_MOVE_DONE,
                TransportOrderStateOld.PUT_DOWN_START,
                TransportOrderStateOld.PUT_DOWN_DONE,
                TransportOrderStateOld.ENTER_STATION,
                TransportOrderStateOld.AGAIN_MOVE_START,
                TransportOrderStateOld.AGAIN_MOVE_DONE,
                TransportOrderStateOld.LOAD_COMPLETED,
                TransportOrderStateOld.PROCESS);
    }

    /**
     * 中控作业单列表显示待执行状态
     *
     * @return
     */
    public static List<String> getConsoleWaitingState() {
        return CONSOLE_WAITING_STATE;
    }

    /**
     * 中控作业单列表显示执行中状态
     *
     * @return
     */
    public static List<String> getConsoleExecutingState() {
        return CONSOLE_EXECUTING_STATE;
    }

    /**
     * 中控作业单列表显示异常状态
     *
     * @return
     */
    public static List<String> getConsoleExceptionState() {
        return CONSOLE_EXCEPTION_STATE;
    }

    /**
     * 中控作业单列表显示完成状态
     *
     * @return
     */
    public static List<String> getConsoleDoneState() {
        return CONSOLE_DONE_STATE;
    }

    // /**
    //  * 腾挪任务故障用的
    //  *
    //  * @return
    //  */
    // public static List<String> unDoneWorkBinStatus() {
    //     return Arrays.asList(WAITING_NEXTSTOP.name(), WAITING_ROBOT.name(), WAITING_DISPATCHER.name());
    // }
    //
    // /**
    //  * 获取任务组工作中的状态
    //  *
    //  * @return
    //  */
    // public static List<String> getJobGroupWorkingStates() {
    //     return Lists.newArrayList(
    //             WAITING_NEXTSTOP.name(),
    //             WAITING_HDS.name(),
    //             DOING_HDS.name(),
    //             WAITING_ROBOT.name(),
    //             WAITING_LIFT.name(),
    //             INIT_JOB.name(), LIFT_UP_DONE.name(), MOVE_BEGIN.name(), PUT_DOWN_DONE.name());
    // }
}
