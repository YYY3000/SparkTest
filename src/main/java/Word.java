import java.io.Serializable;

/**
 * @author yinyiyun
 * @date 2018/6/6 17:11
 */
public class Word implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private Integer count;

    public Word(String name, Integer count) {
        this.name = name;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}