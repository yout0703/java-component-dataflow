package yout.component.dataflow.context;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.tree.Tree;
import cn.hutool.core.map.MapUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @description: Dataflow 上下文，用来存储所有注册上的flow和step
 * @author: yout0703
 * @date: 2023-07-05
 */
@Component
@Slf4j
public class DataflowContext {
    public static boolean initialized = false;

    /**
     * 存储所有 Dataflow 的步骤树形结构
     */
    private final Map<String, List<Tree<String>>> dataflowTreeMap = new ConcurrentHashMap<>();

    /**
     * 存储一个任务下的所有步骤
     */
    @Getter
    private final Map<String, List<String>> dataFlowJobsMap = new ConcurrentHashMap<>();

    public void addDataFlowJobsMap(String dataflowName, List<String> jobNames) {
        dataFlowJobsMap.put(dataflowName, jobNames);
    }

    /**
     * 添加 Dataflow
     *
     * @param dataflowName
     * @param dataflowStepTree
     */
    public void addDataflowTree(String dataflowName, List<Tree<String>> dataflowStepTree) {
        dataflowTreeMap.put(dataflowName, dataflowStepTree);
    }

    /**
     * 获取对应flow的stepTree
     *
     * @param dataflowName
     * @return
     */
    public List<Tree<String>> getDataflowStepTree(String dataflowName) {
        return dataflowTreeMap.get(dataflowName);
    }

    /**
     * 获取一个flow的入口方法
     *
     * @param dataflowName
     * @return
     */
    public List<String> getEntranceSteps(String dataflowName) {
        List<Tree<String>> trees = dataflowTreeMap.get(dataflowName);
        if (CollUtil.isNotEmpty(trees)) {
            return trees.stream()
                        .map(t -> String.valueOf(t.getName()))
                        .collect(Collectors.toList());
        }
        return List.of();
    }

    public List<String> getNextSteps(String dataflowName, String currentStepName) {
        if (MapUtil.isEmpty(dataflowTreeMap) || dataflowTreeMap.get(dataflowName) == null) {
            return List.of();
        }
        var tree = dataflowTreeMap.get(dataflowName);
        for (Tree<String> stringTree : tree) {
            Tree<String> node = stringTree.getNode(currentStepName);
            if (node == null) {
                continue;
            }
            List<Tree<String>> children = node.getChildren();
            if (CollUtil.isEmpty(children)) {
                continue;
            }
            return children.stream()
                           .map(Tree::getId)
                           .collect(Collectors.toList());
        }
        return List.of();
    }
}
