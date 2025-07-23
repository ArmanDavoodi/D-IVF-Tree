#ifndef DATASTRUCTURES_H_
#define DATASTRUCTURES_H_

namespace divftree {

template<typename IteratorType, typename ValueType>
class IteratorWrapper {
public:
    IteratorWrapper(IteratorType begin, IteratorType end)
        : _begin(begin), _end(end) {}
    ~IteratorWrapper() = default;

    inline bool HasNext() const {
        return _begin != _end;
    }

    inline ValueType operator*() const {
        return *_begin;
    }

    inline IteratorWrapper& operator++() {
        ++_begin;
        return *this;
    }

private:
    IteratorType _begin;
    IteratorType _end;
};


};

#endif // DATASTRUCTURES_H_