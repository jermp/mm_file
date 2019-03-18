#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <type_traits>
#include <fcntl.h>
#include <unistd.h>  // close(fd)
#include <string>

namespace mm {

namespace advice {
static const int normal = POSIX_MADV_NORMAL;
static const int random = POSIX_MADV_RANDOM;
static const int sequential = POSIX_MADV_SEQUENTIAL;
}  // namespace advice

template <typename T = char>
struct file_source {
    static_assert(std::is_pod<T>::value, "T must be a POD");

    file_source() {
        init();
    }

    file_source(std::string const& path, int adv = advice::normal) {
        init();
        open(path, adv);
    }

    ~file_source() {
        close();
    }

    void open(std::string const& path, int adv = advice::normal) {
        m_fd = ::open(path.c_str(), O_RDONLY);
        if (m_fd == -1) {
            throw std::runtime_error("cannot open file");
        }

        struct stat fs;
        if (fstat(m_fd, &fs) == -1) {
            throw std::runtime_error("cannot stat file");
        }
        m_size = fs.st_size;

        // map entire file starting from the beginning (offset 0)
        m_data = static_cast<T const*>(
            mmap(NULL, m_size, PROT_READ, MAP_SHARED, m_fd, 0));
        if (m_data == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }

        if (posix_madvise((void*)m_data, m_size / sizeof(T), adv)) {
            throw std::runtime_error("madvise failed");
        }
    }

    bool is_open() const {
        return m_fd != -1;
    }

    void close() {
        if (is_open()) {
            if (munmap((char*)m_data, m_size) == -1) {
                throw std::runtime_error(
                    "munmap failed when closing file_source");
            }
            ::close(m_fd);
            init();
        }
    }

    size_t bytes() const {
        return m_size;
    }

    size_t size() const {
        return m_size / sizeof(T);
    }

    T const* data() const {
        return m_data;
    }

    struct iterator {
        iterator(T const* addr, size_t offset = 0) : m_ptr(addr + offset) {}

        T operator*() {
            return *m_ptr;
        }

        void operator++() {
            ++m_ptr;
        }

        bool operator==(iterator const& rhs) const {
            return m_ptr == rhs.m_ptr;
        }

        bool operator!=(iterator const& rhs) const {
            return !((*this) == rhs);
        }

    private:
        T const* m_ptr;
    };

    iterator begin() const {
        return iterator(m_data);
    }

    iterator end() const {
        return iterator(m_data, size());
    }

private:
    int m_fd;
    size_t m_size;
    T const* m_data;

    void init() {
        m_fd = -1;
        m_size = 0;
        m_data = nullptr;
    }
};

template <typename T = char>
struct file_sink {
    static_assert(std::is_pod<T>::value, "T must be a POD");

    file_sink() {
        init();
    }

    file_sink(std::string const& path, size_t n) {
        init();
        open(path, n);
    }

    ~file_sink() {
        close();
    }

    void open(std::string const& path, size_t n) {
        static const mode_t mode = 0600;
        m_fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, mode);
        if (m_fd == -1) {
            throw std::runtime_error("cannot open file");
        }

        m_size = n * sizeof(T);
        // truncate the file at the new size
        ftruncate(m_fd, m_size);

        // map [m_size] bytes starting from the beginning (offset 0)
        m_data = static_cast<T*>(
            mmap(NULL, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0));
        if (m_data == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }
    }

    bool is_open() const {
        return m_fd != -1;
    }

    void close() {
        if (is_open()) {
            if (munmap((char*)m_data, m_size) == -1) {
                throw std::runtime_error(
                    "munmap failed when closing file_sink");
            }
            ::close(m_fd);
            init();
        }
    }

    size_t bytes() const {
        return m_size;
    }

    size_t size() const {
        return m_size / sizeof(T);
    }

    T* data() const {
        return m_data;
    }

    struct iterator {
        iterator(T* addr, size_t offset = 0) : m_ptr(addr + offset) {}

        T operator*() {
            return *m_ptr;
        }

        void operator++() {
            ++m_ptr;
        }

        bool operator==(iterator const& rhs) const {
            return m_ptr == rhs.m_ptr;
        }

        bool operator!=(iterator const& rhs) const {
            return !((*this) == rhs);
        }

    private:
        T* m_ptr;
    };

    iterator begin() const {
        return iterator(m_data);
    }

    iterator end() const {
        return iterator(m_data, size());
    }

private:
    int m_fd;
    size_t m_size;
    T* m_data;

    void init() {
        m_fd = -1;
        m_size = 0;
        m_data = nullptr;
    }
};

}  // namespace mm