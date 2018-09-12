// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#include <boost/iostreams/device/mapped_file.hpp>
#include <mutex>

namespace mapreduce {

namespace datasource {

namespace detail {

template<typename Key, typename Value>
class file_handler : ::mapreduce::detail::noncopyable
{
  public:
    explicit file_handler(mapreduce::specification const &spec);

    bool const get_data(Key const &key, Value &value)   const;
    bool const setup_key(Key &/*key*/)                  const { return false; }

  private:
    mapreduce::specification const &specification_;

    struct data;
    std::shared_ptr<data> data_;
};

template<>
struct file_handler<
    std::string,
    std::ifstream>::data
{
};

template<>
file_handler<
    std::string,
    std::ifstream>::file_handler(mapreduce::specification const &spec)
  : specification_(spec), data_(new data)
{
}

template<>
bool const
file_handler<
    std::string,
    std::ifstream>::get_data(
        std::string const &key,
        std::ifstream &value) const
{
    value.open(key.c_str(), std::ios_base::binary);
    return value.is_open();
}


template<>
struct file_handler<
    std::string,
    std::pair<
        char const *,
        std::uintmax_t> >::data
{
    struct detail
    {
        boost::iostreams::mapped_file mmf;    // memory mapped file
        std::streamsize               size;   // size of the file
        std::streamsize               offset; // offset to map next time
    };

    typedef
    std::map<std::string, std::shared_ptr<detail> >
    maps_t;

    maps_t      maps;
    std::mutex  mutex;
    std::string current_file;
};

template<>
file_handler<
    std::string,
    std::pair<
        char const *,
        std::uintmax_t> >::file_handler(mapreduce::specification const &spec)
  : specification_(spec), data_(new data)
{
}


template<>
bool const
file_handler<
    std::string,
    std::pair<
        char const *,
        std::uintmax_t> >::get_data(
            std::string const &key,
            std::pair<char const *, std::uintmax_t> &value) const
{
    // we need to hold the lock for the duration of this function
    std::lock_guard<std::mutex> l(data_->mutex);
    data::maps_t::const_iterator it;
    if (data_->current_file.empty())
    {
        data_->current_file = key;
        it = data_->maps.insert(std::make_pair(key, std::make_shared<data::detail>())).first;
        auto &detail = it->second;
        auto &mmf = detail->mmf;
        mmf.open(key, BOOST_IOS::in);
        if (!mmf.is_open())
        {
            std::cerr << "\nFailed to map file into memory: " << key;
            return false;
        }

        detail->size   = boost::filesystem::file_size(key);
        detail->offset = std::min(specification_.max_file_segment_size, detail->size);
        value.first    = mmf.const_data();
        value.second   = detail->offset;
    }
    else
    {
        assert(key == data_->current_file);
        it = data_->maps.find(key);
        assert(it != data_->maps.end());
        auto &detail = it->second;

        std::uintmax_t const new_offset = std::min(detail->offset+specification_.max_file_segment_size, detail->size); 
        value.first  = detail->mmf.const_data() + detail->offset;
        value.second = new_offset - detail->offset;
        detail->offset = new_offset;
    }

    auto &detail = it->second;
    if (detail->offset == detail->size)
        data_->current_file.clear();
    else
    {
        // break on a line boundary
        char const *ptr = value.first + value.second;
        while (*ptr != '\n'  &&  *ptr != '\r'  &&  detail->offset != detail->size)
        {
            ++ptr;
            ++value.second;
            ++detail->offset;
        }
    }

    return true;
}

template<>
bool const
file_handler<
    std::string,
    std::pair<
        char const *,
        std::uintmax_t> >::setup_key(std::string &key) const
{
    std::lock_guard<std::mutex> l(data_->mutex);
    if (data_->current_file.empty())
        return false;
    key = data_->current_file;
    return true;
}

}   // namespace detail

template<
    typename MapTask,
    typename FileHandler = detail::file_handler<
        typename MapTask::key_type,
        typename MapTask::value_type> >
class directory_iterator : mapreduce::detail::noncopyable
{
  public:
    explicit directory_iterator(mapreduce::specification const &spec)
      : specification_(spec),
        file_handler_(spec)
    {
        it_dir_ = it_dir_t(specification_.input_directory);
    }

    bool const setup_key(typename MapTask::key_type &key) const
    {
        if (!file_handler_.setup_key(key))
        {
            while (it_dir_ != it_dir_t()
                && boost::filesystem::is_directory(*it_dir_))
            {
                ++it_dir_;
            }

            if (it_dir_ == it_dir_t())
                return false;

            path_t path = *it_dir_++;
            key = path.string();
        }
        return true;
    }

    bool const get_data(typename MapTask::key_type &key, typename MapTask::value_type &value) const
    {
        return file_handler_.get_data(key, value);
    }

  private:
    typedef boost::filesystem::path path_t;
    typedef boost::filesystem::directory_iterator it_dir_t;

    mutable it_dir_t                it_dir_;
    std::string                     directory_;
    mapreduce::specification const &specification_;
    FileHandler                     file_handler_;
};

}   // namespace datasource

}   // namespace mapreduce 

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
