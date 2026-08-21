# log-reader ai_apikeytags 字段格式变更设计变更

## 背景

`log-reader` 输出的 `ai_apikeytags` 字段由原来的 **数组** 格式：

```json
[
    {"tagname": "dep", "tagvalue": "op"},
    {"tagname": "team", "tagvalue": "bfe"}
]
```

变更为 **按 level 分层的对象** 格式：

```json
{
    "level1": {
        "tagname": "dep",
        "tagvalue": "op"
    },
    "level2": {
        "tagname": "team",
        "tagvalue": "bfe"
    },
    "level3": {
        "tagname": "person",
        "tagvalue": "zhangsan"
    },
    "level4": {},
    "level5": {}
}
```

文档已同步更新：`log-reader/doc/modules/mod_kafka/output-fields.md`。

## 影响范围

- `bfe-access-pb` v0.3.0 中的 `ApikeyTag` 已新增 `taglevel` 字段（`int32`），用于标识标签所属层级。
- `log-reader` 当前代码中 `ai_apikeytags` 仍按 `[]ApikeyTagJSON` 数组输出，需要改为对象输出。

## 修改目标

1. `reader_modules/mod_kafka/field_registry.go` 中 `ai_apikeytags` 字段注册类型由 `[]object` 改为 `object`。
2. `ai_apikeytags` 的提取逻辑由 `[]ApikeyTagJSON` 改为 `map[string]interface{}`（固定 `level1` ~ `level5` 五个 key）。
3. 新增/调整 `isZero` 判断，使该字段被选中时始终输出（含空 `level4`/`level5`）。
4. 同步更新单元测试与集成测试数据及断言。

## 详细改动

### 1. JSON 输出类型定义（`reader_modules/mod_kafka/json_converter.go`）

保留已有的 `ApikeyTagJSON` 作为单个标签对象：

```go
// ApikeyTagJSON API Key tag
type ApikeyTagJSON struct {
    Tagname  string `json:"tagname"`
    Tagvalue string `json:"tagvalue"`
}
```

无需新增结构体，外层对象直接使用 `map[string]interface{}`。

### 2. 字段注册与提取（`reader_modules/mod_kafka/field_registry.go`）

#### 2.1 新增/调整 isZero 函数

增加一个用于对象类型（map）的零值判断：

```go
func isZeroObject(v interface{}) bool {
    return v == nil
}
```

若后续需要按“是否所有 level 均为空”来判断零值，可扩展该函数；当前按文档示例，字段输出时保留 `level1`~`level5`，因此直接判 nil 即可。

#### 2.2 移除数组分支（可选）

`isZeroSlice` 中的 `case []ApikeyTagJSON` 分支不再需要，可删除。

#### 2.3 修改 ai_apikeytags 注册

当前代码：

```go
registerField("ai_apikeytags", "[]object", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            tags := reqLog.GetAiApikeytags()
            result := make([]ApikeyTagJSON, 0, len(tags))
            for _, t := range tags {
                result = append(result, ApikeyTagJSON{
                    Tagname:  t.GetTagname(),
                    Tagvalue: t.GetTagvalue(),
                })
            }
            return result
        }
        return []ApikeyTagJSON{}
    },
    isZeroSlice,
)
```

建议改为：

```go
registerField("ai_apikeytags", "object", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        result := map[string]interface{}{
            "level1": map[string]interface{}{},
            "level2": map[string]interface{}{},
            "level3": map[string]interface{}{},
            "level4": map[string]interface{}{},
            "level5": map[string]interface{}{},
        }
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            for _, t := range reqLog.GetAiApikeytags() {
                lvl := t.GetTaglevel()
                if lvl < 1 || lvl > 5 {
                    continue
                }
                key := fmt.Sprintf("level%d", lvl)
                result[key] = ApikeyTagJSON{
                    Tagname:  t.GetTagname(),
                    Tagvalue: t.GetTagvalue(),
                }
            }
        }
        return result
    },
    isZeroObject,
)
```

> **注意**：对于一条访问日志，同一 `level` 不会出现多个 `ApikeyTag`，因此无需处理冲突或覆盖场景。

### 3. 单元测试更新

#### 3.1 `reader_modules/mod_kafka/field_registry_test.go`

在 `makeBfeLog()` 的 `RequestLog` 中为 `AiApikeytags` 增加带 `Taglevel` 的测试数据：

```go
AiApikeytags: []*bfe_access_pb.ApikeyTag{
    {Tagname: strPtr("dep"), Tagvalue: strPtr("ops"), Taglevel: int32Ptr(1)},
    {Tagname: strPtr("team"), Tagvalue: strPtr("bfe"), Taglevel: int32Ptr(2)},
},
```

并补充 `int32Ptr` 辅助函数（若不存在）。

新增测试用例 `TestFieldRegistry_ExtractAiApikeytags`：

```go
func TestFieldRegistry_ExtractAiApikeytags(t *testing.T) {
    log := makeBfeLog()
    val, isZero := Extract("ai_apikeytags", log)
    if isZero {
        t.Fatal("ai_apikeytags should not be zero")
    }
    tags, ok := val.(map[string]interface{})
    if !ok {
        t.Fatalf("expected map[string]interface{}, got %T", val)
    }
    if len(tags) != 5 {
        t.Fatalf("expected 5 levels, got %d", len(tags))
    }
    lvl1, ok := tags["level1"].(ApikeyTagJSON)
    if !ok || lvl1.Tagname != "dep" || lvl1.Tagvalue != "ops" {
        t.Errorf("unexpected level1: %+v", tags["level1"])
    }
    lvl2, ok := tags["level2"].(ApikeyTagJSON)
    if !ok || lvl2.Tagname != "team" || lvl2.Tagvalue != "bfe" {
        t.Errorf("unexpected level2: %+v", tags["level2"])
    }
    // level3~level5 应为空 map
    for _, lvl := range []string{"level3", "level4", "level5"} {
        m, ok := tags[lvl].(map[string]interface{})
        if !ok || len(m) != 0 {
            t.Errorf("expected %s to be empty object, got %+v", lvl, tags[lvl])
        }
    }
}
```

#### 3.2 `reader_modules/mod_kafka/json_converter_test.go`

当前该文件未直接断言 `ai_apikeytags`，无需修改。若后续补充断言，应使用反序列化后的 `map[string]interface{}` 检查对象结构。

### 4. 集成测试更新

#### 4.1 测试数据（`tests/integration/common/log_generator.go`）

将：

```go
AiApikeytags: []*bfe_access_pb.ApikeyTag{
    {Tagname: strPtr("dep"), Tagvalue: strPtr("ops")},
    {Tagname: strPtr("team"), Tagvalue: strPtr("bfe")},
},
```

改为：

```go
AiApikeytags: []*bfe_access_pb.ApikeyTag{
    {Tagname: strPtr("dep"), Tagvalue: strPtr("ops"), Taglevel: int32Ptr(1)},
    {Tagname: strPtr("team"), Tagvalue: strPtr("bfe"), Taglevel: int32Ptr(2)},
},
```

#### 4.2 断言（`tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go`）

原断言：

```go
assertFieldObjectArrayEquals(t, payload, "ai_apikeytags", []map[string]interface{}{
    {"tagname": "dep", "tagvalue": "ops"},
    {"tagname": "team", "tagvalue": "bfe"},
})
```

建议新增对象断言辅助函数：

```go
// assertFieldObjectEquals checks that payload[field] equals want as an object.
func assertFieldObjectEquals(t *testing.T, payload map[string]interface{}, field string, want map[string]interface{}) {
    t.Helper()
    got, ok := payload[field]
    if !ok {
        t.Errorf("missing field %q", field)
        return
    }
    if !reflect.DeepEqual(got, want) {
        t.Errorf("field %q = %v, want %v", field, got, want)
    }
}
```

并替换原断言为：

```go
assertFieldObjectEquals(t, payload, "ai_apikeytags", map[string]interface{}{
    "level1": map[string]interface{}{"tagname": "dep", "tagvalue": "ops"},
    "level2": map[string]interface{}{"tagname": "team", "tagvalue": "bfe"},
    "level3": map[string]interface{}{},
    "level4": map[string]interface{}{},
    "level5": map[string]interface{}{},
})
```

> 需要引入 `reflect` 包。

### 5. 验证步骤

1. 单元测试：

```bash
cd log-reader
go test ./reader_modules/mod_kafka/...
```

2. 编译：

```bash
go build ./...
```

3. 集成测试（依赖环境已就绪时执行）：

```bash
cd tests/integration
go test ./implementation/scenario-LR01-basic-flow/...
```

## 风险与兼容说明

- 该字段对外 JSON 格式由数组变为对象，属于 **破坏性变更**。下游消费方需同步调整解析逻辑。
- `bfe-access-pb` v0.3.0 已提供 `taglevel`，本方案依赖该字段。若上游日志无 `taglevel`，对应标签会被忽略，输出空 `level` 对象。
- 当前 `ConvertBfeLogToJSON` 不会过滤零值字段，因此空 `level4`/`level5` 仍会输出；若后续开启零值过滤，`isZeroObject` 需要重新评估是否保留该字段。
